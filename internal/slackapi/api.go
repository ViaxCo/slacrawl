package slackapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

const (
	SourceUser = "api-user"
	SourceBot  = "api-bot"

	// defaultHTTPTimeout bounds Slack API HTTP when NewWithOptions gets a nil client.
	defaultHTTPTimeout = 60 * time.Second
)

func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: defaultHTTPTimeout}
}

type Diagnostics struct {
	BotConfigured     bool   `json:"bot_configured"`
	AppConfigured     bool   `json:"app_configured"`
	UserConfigured    bool   `json:"user_configured"`
	ThreadCoverage    string `json:"thread_coverage"`
	DMsIncluded       bool   `json:"dms_included"`
	DMsMissingScope   string `json:"dms_missing_scope,omitempty"`
	BotAuthTeamID     string `json:"bot_auth_team_id,omitempty"`
	BotAuthTeam       string `json:"bot_auth_team,omitempty"`
	UserAuthAvailable bool   `json:"user_auth_available"`
	UserAuthError     string `json:"user_auth_error,omitempty"`
	AppTailAvailable  bool   `json:"app_tail_available"`
}

type SyncOptions struct {
	WorkspaceID      string
	Channels         []string
	ExcludeChannels  []string
	Since            string
	Full             bool
	LatestOnly       bool
	Concurrency      int
	AutoJoin         *bool
	enforceRetention bool
	ordinarySync     bool
}

type Client struct {
	bot          *slack.Client
	user         *slack.Client
	tokens       config.Tokens
	appToken     string
	apiURL       string
	httpClient   *http.Client
	dmPolicy     admission.DMPolicy
	sleep        func(context.Context, time.Duration) error
	now          func() time.Time
	socketModeFn func(*slack.Client) socketModeRunner
	logger       *slog.Logger
}

func New(tokens config.Tokens) *Client {
	return NewWithOptions(tokens, "", nil)
}

func NewWithOptions(tokens config.Tokens, apiURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = defaultHTTPClient()
	}
	client := &Client{
		tokens:     tokens,
		appToken:   tokens.App,
		apiURL:     slack.APIURL,
		httpClient: httpClient,
		sleep:      sleepContext,
		now:        func() time.Time { return time.Now().UTC() },
	}
	if apiURL != "" {
		client.apiURL = apiURL
	}

	buildOptions := func(includeAppToken bool) []slack.Option {
		var options []slack.Option
		if apiURL != "" {
			options = append(options, slack.OptionAPIURL(apiURL))
		}
		options = append(options, slack.OptionHTTPClient(httpClient))
		if includeAppToken && tokens.App != "" {
			options = append(options, slack.OptionAppLevelToken(tokens.App))
		}
		return options
	}

	if tokens.Bot != "" {
		client.bot = slack.New(tokens.Bot, buildOptions(true)...)
	}
	if tokens.User != "" {
		client.user = slack.New(tokens.User, buildOptions(false)...)
	}
	client.socketModeFn = func(api *slack.Client) socketModeRunner {
		return managedSocketMode{client: socketmode.New(api)}
	}
	return client
}

func (c *Client) WithDMPolicy(policy admission.DMPolicy) *Client {
	c.dmPolicy = policy
	return c
}

func (c *Client) WithLogger(logger *slog.Logger) *Client {
	c.logger = logger
	return c
}

func (c *Client) Doctor(ctx context.Context) (Diagnostics, error) {
	diag := Diagnostics{
		BotConfigured:  c.tokens.Bot != "",
		AppConfigured:  c.tokens.App != "",
		UserConfigured: c.tokens.User != "",
		ThreadCoverage: "partial",
	}
	if c.bot != nil {
		resp, err := c.authTest(ctx, c.bot)
		if err != nil {
			return diag, err
		}
		diag.BotAuthTeamID = resp.TeamID
		diag.BotAuthTeam = resp.Team
		diag.AppTailAvailable = c.tokens.App != ""
	}

	if c.user != nil {
		userAuth, err := c.authTest(ctx, c.user)
		if err == nil {
			_, err = authenticatedWorkspaceID(userAuth, diag.BotAuthTeamID)
		}
		if err == nil {
			diag.UserAuthAvailable = true
			diag.ThreadCoverage = "full"
			if c.dmPolicy.Enabled(c.tokens.User != "") {
				diag.DMsIncluded = true
				diag.DMsMissingScope = c.dmMissingScope(ctx, userAuth.TeamID)
			}
		} else {
			diag.UserAuthError = authErrorReason(err)
		}
	}
	return diag, nil
}

func (c *Client) Sync(ctx context.Context, st *store.Store, opts SyncOptions) error {
	// Empty Since alone also describes Tail repair; only ordinary Sync owns
	// the durable retained-thread backlog.
	opts.ordinarySync = true
	// Selection is invocation-local: Tail and its repair path still own the bot.
	source := channelSyncSource{
		historyClient: c.bot, token: c.tokens.Bot, sourceName: SourceBot,
		sourceRank: 2, allowJoin: syncAutoJoin(opts),
	}
	if source.historyClient == nil {
		source = channelSyncSource{
			historyClient: c.user, token: c.tokens.User, sourceName: SourceUser, sourceRank: 1,
		}
	}
	if source.historyClient == nil {
		return errors.New("SLACK_BOT_TOKEN or SLACK_USER_TOKEN is required for api sync")
	}

	auth, err := c.authTest(ctx, source.historyClient)
	if err != nil {
		return err
	}
	workspaceID, err := authenticatedWorkspaceID(auth, opts.WorkspaceID)
	if err != nil {
		return err
	}
	userRepliesAvailable := source.sourceName == SourceUser
	if !userRepliesAvailable {
		userRepliesAvailable, err = c.userAuthAvailable(ctx, workspaceID)
		if err != nil {
			return err
		}
	}

	now := c.now()
	if err := st.UpsertWorkspace(ctx, store.Workspace{
		ID:           workspaceID,
		Name:         auth.Team,
		EnterpriseID: auth.EnterpriseID,
		RawJSON:      store.MarshalRaw(auth),
		UpdatedAt:    now,
	}); err != nil {
		return err
	}
	threadRepliesSkipped := newThreadSkipTracker()
	source.threadSkip = threadRepliesSkipped

	channels, err := c.fetchChannelsWithClient(ctx, source.historyClient, workspaceID)
	if err != nil {
		return err
	}
	allow := make(map[string]struct{}, len(opts.Channels))
	for _, id := range opts.Channels {
		allow[id] = struct{}{}
	}
	excluded := excludedChannelNames(opts.ExcludeChannels)
	selectedChannels := make([]slack.Channel, 0, len(channels))
	for _, channel := range channels {
		if len(allow) > 0 {
			if _, ok := allow[channel.ID]; !ok {
				continue
			}
		}
		if channelExcluded(channel, excluded) {
			continue
		}
		selectedChannels = append(selectedChannels, channel)
	}
	if err := c.syncChannelsWithSource(ctx, st, workspaceID, selectedChannels, opts, now, userRepliesAvailable, source); err != nil {
		return err
	}

	var (
		users    []slack.User
		userByID map[string]slack.User
	)
	if c.dmPolicy.Enabled(c.tokens.User != "") && userRepliesAvailable && c.user != nil {
		users, err = c.getUsers(ctx, source.historyClient)
		if err != nil {
			return err
		}
		userByID = make(map[string]slack.User, len(users))
		for _, user := range users {
			userByID[user.ID] = user
		}

		dms, err := c.fetchDMs(ctx, workspaceID)
		if err != nil {
			if isMissingScopeError(err) {
				if setErr := st.SetSyncState(ctx, SourceUser, "dms", workspaceID, "missing_scope"); setErr != nil {
					return setErr
				}
			} else {
				return err
			}
		} else {
			selectedDMs := make([]slack.Channel, 0, len(dms))
			for _, channel := range dms {
				if len(allow) > 0 {
					if _, ok := allow[channel.ID]; !ok {
						continue
					}
				}
				channel.Name = dmChannelName(channel, userByID)
				if channelExcluded(channel, excluded) {
					continue
				}
				selectedDMs = append(selectedDMs, channel)
			}
			if err := c.syncChannelsWithSource(ctx, st, workspaceID, selectedDMs, opts, now, userRepliesAvailable, channelSyncSource{
				historyClient:    c.user,
				token:            c.tokens.User,
				sourceName:       SourceUser,
				sourceRank:       1,
				allowJoin:        false,
				skipMissingScope: true,
				threadSkip:       threadRepliesSkipped,
			}); err != nil {
				return err
			}
		}
	}

	if users == nil {
		users, err = c.getUsers(ctx, source.historyClient)
		if err != nil {
			return err
		}
	}
	for _, user := range users {
		if _, err := c.skipUserCollision(ctx, st, workspaceID, user, now); err != nil {
			return err
		}
	}

	threadCoverage := "partial"
	if userRepliesAvailable && !threadRepliesSkipped.Skipped() {
		if opts.Full && len(opts.Channels) == 0 {
			if err := st.DeleteAPIThreadSkipsIfNoPending(ctx, workspaceID); err != nil {
				return err
			}
		}
		hasThreadSkips, err := st.HasSyncStateType(ctx, SourceUser, "thread_skip")
		if err != nil {
			return err
		}
		hasPendingThreads, err := st.HasSyncStateType(ctx, SourceUser, store.ThreadPendingEntityType)
		if err != nil {
			return err
		}
		if !hasThreadSkips && !hasPendingThreads {
			threadCoverage = "full"
		}
	}
	if err := st.SetSyncState(ctx, "doctor", "threads", "coverage", threadCoverage); err != nil {
		return err
	}
	return st.SetSyncState(ctx, source.sourceName, "workspace", workspaceID, now.Format(time.RFC3339))
}

func (c *Client) fetchChannels(ctx context.Context, workspaceID string) ([]slack.Channel, error) {
	return c.fetchChannelsWithClient(ctx, c.bot, workspaceID)
}

func (c *Client) fetchChannelsWithClient(ctx context.Context, client *slack.Client, workspaceID string) ([]slack.Channel, error) {
	var (
		cursor   string
		channels []slack.Channel
		seen     = map[string]bool{}
	)
	for {
		page, nextCursor, err := c.getConversations(ctx, client, &slack.GetConversationsParameters{
			Cursor:          cursor,
			ExcludeArchived: false,
			Limit:           200,
			Types:           []string{"public_channel", "private_channel"},
			TeamID:          workspaceID,
		})
		if err != nil {
			return nil, err
		}
		channels = append(channels, page...)
		if nextCursor == "" {
			return channels, nil
		}
		if seen[nextCursor] {
			return nil, fmt.Errorf("conversations.list repeated cursor %q", nextCursor)
		}
		seen[nextCursor] = true
		cursor = nextCursor
	}
}

func (c *Client) authTest(ctx context.Context, client *slack.Client) (*slack.AuthTestResponse, error) {
	return retry(ctx, c.sleep, 3, func() (*slack.AuthTestResponse, error) {
		return client.AuthTestContext(ctx)
	})
}

func authenticatedWorkspaceID(auth *slack.AuthTestResponse, requested string) (string, error) {
	authTeamID := strings.TrimSpace(auth.TeamID)
	requested = strings.TrimSpace(requested)
	if requested != "" {
		if authTeamID != requested {
			return "", fmt.Errorf("authenticated workspace %s does not match requested workspace %s", authTeamID, requested)
		}
		return requested, nil
	}
	return authTeamID, nil
}

func (c *Client) joinConversation(ctx context.Context, channelID string) error {
	if c.bot == nil {
		return errors.New("SLACK_BOT_TOKEN is required for join")
	}
	_, err := retry(ctx, c.sleep, 3, func() (struct{}, error) {
		_, _, _, err := c.bot.JoinConversationContext(ctx, channelID)
		return struct{}{}, err
	})
	return err
}

func retry[T any](ctx context.Context, sleeper func(context.Context, time.Duration) error, attempts int, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt < attempts; attempt++ {
		value, err := fn()
		if err == nil {
			return value, nil
		}
		var rateLimited *slack.RateLimitedError
		if !errors.As(err, &rateLimited) || attempt == attempts-1 {
			return zero, err
		}
		if err := sleeper(ctx, rateLimited.RetryAfter); err != nil {
			return zero, err
		}
	}
	return zero, errors.New("retry exhausted")
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func repairOldest(latestTS string, overlap time.Duration) string {
	if latestTS == "" {
		return ""
	}
	parsed, err := strconv.ParseFloat(latestTS, 64)
	if err != nil {
		return latestTS
	}
	adjusted := math.Max(parsed-overlap.Seconds(), 0)
	return strconv.FormatFloat(adjusted, 'f', 6, 64)
}
