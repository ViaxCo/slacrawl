package slackapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/crawlkit/progress"
	"github.com/slack-go/slack"

	"github.com/openclaw/slacrawl/internal/store"
)

func (c *Client) syncChannelMessagesWithSource(ctx context.Context, st *store.Store, workspaceID string, channel slack.Channel, oldest string, restoreRequested bool, now time.Time, userRepliesAvailable bool, source channelSyncSource) error {
	if source.historyClient == nil {
		return errors.New("history client is required")
	}
	if source.sourceName == "" {
		source.sourceName = SourceBot
	}
	if source.sourceRank == 0 {
		source.sourceRank = 2
	}
	retentionFloor, err := st.ChannelRetentionFloor(ctx, workspaceID, channel.ID)
	if err != nil {
		return err
	}
	enforceRetention := store.ShouldEnforceRetention(oldest, retentionFloor, restoreRequested)
	if !enforceRetention {
		retentionFloor = ""
	}
	coverage, err := loadHistoryCoverage(ctx, st, source.sourceName, workspaceID, channel.ID, source.coverageScope)
	if err != nil {
		return err
	}
	coverage.Pending = &oldest
	if err := saveHistoryCoverage(ctx, st, source.sourceName, workspaceID, channel.ID, source.coverageScope, coverage); err != nil {
		return err
	}
	horizon := fmt.Sprintf("%d.%06d", now.Unix(), now.Nanosecond()/1000)
	inclusive := retentionFloor != "" && oldest == retentionFloor
	syncedThreads := map[string]struct{}{}
	pendingThreads := map[string]store.ThreadWork{}
	knownThreads := map[string]struct{}{}
	var discovery *store.ThreadWorkDiscovery
	if source.retainedThreads {
		discovery = &store.ThreadWorkDiscovery{SourceName: SourceUser, WorkspaceID: workspaceID, ChannelID: channel.ID, ExcludedTS: knownThreads}
		work, err := st.PrepareThreadWork(ctx, SourceUser, workspaceID, channel.ID)
		if err != nil {
			return err
		}
		for _, item := range work {
			pendingThreads[item.TS] = item
			knownThreads[item.TS] = struct{}{}
		}
	}
	syncThreadOnce := func(threadTS string) error {
		if _, ok := syncedThreads[threadTS]; ok {
			return nil
		}
		syncedThreads[threadTS] = struct{}{}
		knownThreads[threadTS] = struct{}{}
		threadKey := workspaceID + "|" + channel.ID + "|" + threadTS
		var work *store.ThreadWork
		if pending, ok := pendingThreads[threadTS]; ok {
			work = &pending
		}
		saveSkip := func(reason string) (bool, error) {
			if work == nil {
				return true, st.SetSyncState(ctx, SourceUser, "thread_skip", threadKey, reason)
			}
			result, err := st.ApplyWriteBatch(ctx, store.WriteBatch{
				ThreadGuard: work,
				SyncStates:  []store.SyncStateWrite{{SourceName: SourceUser, EntityType: "thread_skip", EntityID: threadKey, Value: reason}},
			})
			return !result.ThreadWorkRevoked, err
		}
		if source.threadSkip != nil {
			if reason, ok := source.threadSkip.SkipReason(channel.ID, threadSkipScope(channel)); ok {
				_, err := saveSkip(reason)
				return err
			}
		}
		outcome, err := c.syncThread(ctx, st, workspaceID, channel.ID, threadTS, enforceRetention, now, work)
		if err != nil {
			if isThreadRepliesSkipped(err) {
				saved, saveErr := saveSkip(channelSkipReason(err))
				if saveErr != nil {
					return saveErr
				}
				if saved && source.threadSkip != nil {
					source.threadSkip.Record(channel.ID, threadSkipScope(channel), channelSkipReason(err))
				}
				return nil
			}
			return err
		}
		if outcome == threadSyncRevoked {
			return nil
		}
		if work != nil {
			return st.CompleteThreadWork(ctx, *work, threadKey)
		}
		return st.DeleteSyncState(ctx, SourceUser, "thread_skip", threadKey)
	}

	cursor := ""
	seen := map[string]bool{}
	joined := false
	historyLimited := false
	for {
		resp, err := c.getConversationHistory(ctx, source.token, &slack.GetConversationHistoryParameters{
			ChannelID: channel.ID,
			Cursor:    cursor,
			Limit:     200,
			Oldest:    oldest,
			Latest:    horizon,
			Inclusive: inclusive,
		})
		if err != nil {
			if source.skipMissingScope && isMissingScopeError(err) {
				return st.SetSyncState(ctx, source.sourceName, "channel_skip", channel.ID, "missing_scope")
			}
			if source.allowJoin && !joined && channelSkipReason(err) == "not_in_channel" && !channel.IsPrivate {
				joinErr := c.joinConversation(ctx, channel.ID)
				if joinErr == nil {
					joined = true
					if setErr := st.SetSyncState(ctx, source.sourceName, "channel_join", channel.ID, "joined"); setErr != nil {
						return setErr
					}
					continue
				}
				if setErr := st.SetSyncState(ctx, source.sourceName, "channel_join", channel.ID, "failed:"+authErrorReason(joinErr)); setErr != nil {
					return setErr
				}
			}
			if isChannelHistorySkipped(err) {
				return st.SetSyncState(ctx, source.sourceName, "channel_skip", channel.ID, channelSkipReason(err))
			}
			return fmt.Errorf("channel %s history: %w", channel.ID, err)
		}
		historyLimited = historyLimited || resp.IsLimited
		if err := validateMessagePage(resp.Messages, channel.ID); err != nil {
			return fmt.Errorf("channel %s history: %w", channel.ID, err)
		}
		batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, len(resp.Messages)), ThreadDiscovery: discovery}
		threadTSs := make([]string, 0)
		queuedThreads := map[string]struct{}{}
		for _, rawMsg := range resp.Messages {
			msg := rawMsg.Message
			if msg.Channel == "" {
				msg.Channel = channel.ID
			}
			batch.Messages = append(batch.Messages, store.MessageWrite{
				Message:                toStoreMessage(workspaceID, msg, source.sourceName, source.sourceRank, rawMsg.RawPayload, now),
				Mentions:               toStoreMentions(msg),
				EnforceRetention:       enforceRetention,
				SkipWorkspaceCollision: true,
			})
			if source.retainedThreads && msg.ReplyCount > 0 {
				if _, known := knownThreads[msg.Timestamp]; !known {
					batch.PendingThreads = append(batch.PendingThreads, store.ThreadWork{SourceName: SourceUser, WorkspaceID: workspaceID, ChannelID: channel.ID, TS: msg.Timestamp})
				}
			}
			if msg.ReplyCount > 0 && userRepliesAvailable {
				if _, synced := syncedThreads[msg.Timestamp]; !synced {
					if _, queued := queuedThreads[msg.Timestamp]; !queued {
						queuedThreads[msg.Timestamp] = struct{}{}
						threadTSs = append(threadTSs, msg.Timestamp)
					}
				}
			}
		}
		collidedTSs := map[string]struct{}{}
		if len(batch.Messages) > 0 {
			result, err := st.ApplyWriteBatch(ctx, batch)
			if err != nil {
				return err
			}
			for _, skip := range result.CollisionsSkipped {
				c.skipMessageCollision(skip.Err, workspaceID, skip.ChannelID, skip.TS)
				collidedTSs[skip.TS] = struct{}{}
			}
			for _, work := range result.PendingThreads {
				pendingThreads[work.TS] = work
				knownThreads[work.TS] = struct{}{}
			}
		}
		for _, threadTS := range threadTSs {
			// A collided parent belongs to another workspace; its thread does too.
			if _, collided := collidedTSs[threadTS]; collided {
				continue
			}
			if err := syncThreadOnce(threadTS); err != nil {
				return err
			}
		}
		if resp.NextCursor == "" {
			if resp.HasMore {
				return errors.New("conversations.history returned has_more without a continuation cursor; scan remains incomplete; slacrawl does not support timestamp pagination")
			}
			break
		}
		if seen[resp.NextCursor] {
			return fmt.Errorf("conversations.history repeated cursor %q", resp.NextCursor)
		}
		seen[resp.NextCursor] = true
		cursor = resp.NextCursor
	}
	// A later accessible page cannot certify an earlier limited response.
	// Keep the old completed horizon and pending interval for a retry.
	if historyLimited {
		return errors.New("Slack reported a history/message limit; completeness of the requested interval is uncertified; review workspace history availability")
	}
	if source.retainedThreads {
		roots, err := st.ChannelThreadRoots(ctx, workspaceID, channel.ID)
		if err != nil {
			return err
		}
		batch := store.WriteBatch{}
		for _, root := range roots {
			if _, known := knownThreads[root.TS]; !known {
				batch.PendingThreads = append(batch.PendingThreads, store.ThreadWork{SourceName: SourceUser, WorkspaceID: workspaceID, ChannelID: channel.ID, TS: root.TS})
			}
		}
		if len(batch.PendingThreads) > 0 {
			result, err := st.ApplyWriteBatch(ctx, batch)
			if err != nil {
				return err
			}
			for _, work := range result.PendingThreads {
				pendingThreads[work.TS] = work
				knownThreads[work.TS] = struct{}{}
			}
		}
		if userRepliesAvailable {
			threads := make([]string, 0, len(pendingThreads))
			for ts := range pendingThreads {
				threads = append(threads, ts)
			}
			sort.Strings(threads)
			for _, ts := range threads {
				if err := syncThreadOnce(ts); err != nil {
					return err
				}
			}
		}
	}
	coverage.Latest = horizon
	coverage.Complete = true
	coverage.Pending = nil
	return saveHistoryCoverage(ctx, st, source.sourceName, workspaceID, channel.ID, source.coverageScope, coverage)
}

type threadSyncResult uint8

const (
	threadSyncComplete threadSyncResult = iota
	threadSyncRevoked
)

func (c *Client) syncThread(ctx context.Context, st *store.Store, workspaceID string, channelID string, threadTS string, enforceRetention bool, now time.Time, work *store.ThreadWork) (threadSyncResult, error) {
	cursor := ""
	seen := map[string]bool{}
	for {
		if work != nil {
			current, err := st.ThreadWorkCurrent(ctx, *work)
			if err != nil {
				return threadSyncComplete, err
			}
			if !current {
				return threadSyncRevoked, nil
			}
		}
		resp, err := c.getConversationReplies(ctx, &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Cursor:    cursor,
			Limit:     200,
		})
		if work != nil {
			current, checkErr := st.ThreadWorkCurrent(ctx, *work)
			if checkErr != nil {
				return threadSyncComplete, checkErr
			}
			if !current {
				return threadSyncRevoked, nil
			}
		}
		if err != nil {
			return threadSyncComplete, err
		}
		if err := validateMessagePage(resp.Messages, channelID); err != nil {
			return threadSyncComplete, fmt.Errorf("channel %s replies: %w", channelID, err)
		}
		batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, len(resp.Messages)), ThreadGuard: work}
		for _, rawMsg := range resp.Messages {
			msg := rawMsg.Message
			if msg.Channel == "" {
				msg.Channel = channelID
			}
			batch.Messages = append(batch.Messages, store.MessageWrite{
				Message:                toStoreMessage(workspaceID, msg, SourceUser, 1, rawMsg.RawPayload, now),
				Mentions:               toStoreMentions(msg),
				EnforceRetention:       enforceRetention,
				SkipWorkspaceCollision: true,
			})
		}
		// Empty guarded pages also establish whether this generation still owns
		// the response before cursor traversal or completion.
		if len(batch.Messages) > 0 || work != nil {
			result, err := st.ApplyWriteBatch(ctx, batch)
			if err != nil {
				return threadSyncComplete, err
			}
			if result.ThreadWorkRevoked {
				return threadSyncRevoked, nil
			}
			for _, skip := range result.CollisionsSkipped {
				c.skipMessageCollision(skip.Err, workspaceID, skip.ChannelID, skip.TS)
			}
		}
		if resp.NextCursor == "" {
			if resp.HasMore {
				return threadSyncComplete, errors.New("conversations.replies returned has_more without a continuation cursor; scan remains incomplete; slacrawl does not support timestamp pagination")
			}
			return threadSyncComplete, nil
		}
		if seen[resp.NextCursor] {
			return threadSyncComplete, fmt.Errorf("conversations.replies repeated cursor %q", resp.NextCursor)
		}
		seen[resp.NextCursor] = true
		cursor = resp.NextCursor
	}
}

type channelSyncSource struct {
	historyClient    *slack.Client
	token            string
	sourceName       string
	sourceRank       int
	allowJoin        bool
	skipMissingScope bool
	threadSkip       *threadSkipTracker
	coverageScope    string
	retainedThreads  bool
}

func (c *Client) syncChannels(ctx context.Context, st *store.Store, workspaceID string, channels []slack.Channel, opts SyncOptions, now time.Time, userRepliesAvailable bool, threadSkip *threadSkipTracker) error {
	return c.syncChannelsWithSource(ctx, st, workspaceID, channels, opts, now, userRepliesAvailable, channelSyncSource{
		historyClient: c.bot,
		token:         c.tokens.Bot,
		sourceName:    SourceBot,
		sourceRank:    2,
		allowJoin:     syncAutoJoin(opts),
		threadSkip:    threadSkip,
	})
}

func syncAutoJoin(opts SyncOptions) bool {
	return opts.AutoJoin == nil || *opts.AutoJoin
}

func excludedChannelNames(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	excluded := make(map[string]struct{}, len(values))
	for _, value := range values {
		name := normalizeChannelName(value)
		if name != "" {
			excluded[name] = struct{}{}
		}
	}
	return excluded
}

func channelExcluded(channel slack.Channel, excluded map[string]struct{}) bool {
	if len(excluded) == 0 {
		return false
	}
	if _, ok := excluded[normalizeChannelName(channel.Name)]; ok {
		return true
	}
	_, ok := excluded[normalizeChannelName(channel.ID)]
	return ok
}

func normalizeChannelName(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "#"))
}

// skipChannelCollision upserts the channel and reports whether it must be
// skipped because another workspace already owns it. Slack Connect shared
// channels legitimately appear under multiple workspaces, so a collision must
// not abort the whole sync — the workspace that recorded the channel first
// keeps it, and the channel is skipped here with a warning. Any other upsert
// error is returned for the caller's fatal path.
func (c *Client) skipChannelCollision(ctx context.Context, st *store.Store, workspaceID string, channel slack.Channel, now time.Time) (bool, error) {
	err := st.UpsertChannel(ctx, toStoreChannel(workspaceID, channel, now))
	if err == nil {
		return false, nil
	}
	if store.IsWorkspaceCollision(err, "channel") {
		c.warnLogger().Warn("skipping channel owned by another workspace",
			"workspace_id", workspaceID,
			"channel_id", channel.ID,
			"channel_name", channel.Name,
			"err", err,
		)
		return true, nil
	}
	return false, err
}

// skipUserCollision upserts the user and reports whether it must be skipped
// because another workspace already owns it. Enterprise Grid shares user IDs
// org-wide and Slack Connect surfaces external members, so the same user can be
// listed by several workspaces. The workspace that recorded the user first
// keeps it, and later workspaces skip with a warning instead of aborting.
func (c *Client) skipUserCollision(ctx context.Context, st *store.Store, workspaceID string, user slack.User, now time.Time) (bool, error) {
	err := st.UpsertUser(ctx, ToStoreUser(workspaceID, user, now))
	if err == nil {
		return false, nil
	}
	if store.IsWorkspaceCollision(err, "user") {
		c.warnLogger().Warn("skipping user owned by another workspace",
			"workspace_id", workspaceID,
			"user_id", user.ID,
			"user_name", user.Name,
			"err", err,
		)
		return true, nil
	}
	return false, err
}

// skipMessageCollision reports whether a message upsert error is a
// cross-workspace collision the caller should skip rather than propagate.
// A Slack Connect channel is owned by whichever workspace recorded it first, so
// its messages keep arriving for every other member workspace — during history
// sync that must not abort the run, and during tail it must not kill the
// socket-mode loop.
func (c *Client) skipMessageCollision(err error, workspaceID, channelID, ts string) bool {
	if !store.IsWorkspaceCollision(err, "message") {
		return false
	}
	c.warnLogger().Warn("skipping message owned by another workspace",
		"workspace_id", workspaceID,
		"channel_id", channelID,
		"ts", ts,
		"err", err,
	)
	return true
}

func (c *Client) warnLogger() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

func (c *Client) syncChannelsWithSource(ctx context.Context, st *store.Store, workspaceID string, channels []slack.Channel, opts SyncOptions, now time.Time, userRepliesAvailable bool, source channelSyncSource) error {
	if len(channels) == 0 {
		return nil
	}
	channels, err := c.admitChannels(workspaceID, channels)
	if err != nil || len(channels) == 0 {
		return err
	}
	source.coverageScope = opts.Since
	source.retainedThreads = opts.ordinarySync && opts.Since == ""
	channels, oldestByChannel, err := c.channelSyncPlan(ctx, st, workspaceID, channels, opts, source.sourceName)
	if err != nil {
		return err
	}
	if len(channels) == 0 {
		return nil
	}
	workerCount := opts.Concurrency
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(channels) {
		workerCount = len(channels)
	}
	restoreRequested := !opts.enforceRetention && (opts.Since != "" || opts.Full)
	tracker := progress.New(c.logger, progress.Options{
		Name:  "sync",
		Unit:  "channels",
		Total: int64(len(channels)),
		Attrs: []any{"workspace_id", workspaceID, "source", source.sourceName},
	})
	if workerCount == 1 {
		for _, channel := range channels {
			skip, err := c.skipChannelCollision(ctx, st, workspaceID, channel, now)
			if err != nil {
				tracker.Finish(err)
				return err
			}
			if skip {
				continue
			}
			if err := c.syncChannelMessagesWithSource(ctx, st, workspaceID, channel, oldestByChannel[channel.ID], restoreRequested, now, userRepliesAvailable, source); err != nil {
				tracker.Finish(err)
				return err
			}
			tracker.Add(1, "channel_id", channel.ID, "channel_name", channel.Name)
		}
		tracker.Finish(nil)
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workCh := make(chan slack.Channel)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for channel := range workCh {
			skip, err := c.skipChannelCollision(ctx, st, workspaceID, channel, now)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			if skip {
				continue
			}
			if err := c.syncChannelMessagesWithSource(ctx, st, workspaceID, channel, oldestByChannel[channel.ID], restoreRequested, now, userRepliesAvailable, source); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			tracker.Add(1, "channel_id", channel.ID, "channel_name", channel.Name)
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, channel := range channels {
		select {
		case <-ctx.Done():
			close(workCh)
			wg.Wait()
			select {
			case err := <-errCh:
				tracker.Finish(err)
				return err
			default:
				if ctx.Err() != nil {
					tracker.Finish(ctx.Err())
					return ctx.Err()
				}
				tracker.Finish(nil)
				return nil
			}
		case workCh <- channel:
		}
	}
	close(workCh)
	wg.Wait()

	select {
	case err := <-errCh:
		tracker.Finish(err)
		return err
	default:
		if ctx.Err() != nil {
			tracker.Finish(ctx.Err())
			return ctx.Err()
		}
		tracker.Finish(nil)
		return nil
	}
}

func (c *Client) channelSyncPlan(ctx context.Context, st *store.Store, workspaceID string, channels []slack.Channel, opts SyncOptions, sources ...string) ([]slack.Channel, map[string]string, error) {
	out := make(map[string]string, len(channels))
	if opts.Since != "" {
		for _, channel := range channels {
			out[channel.ID] = opts.Since
		}
		return channels, out, nil
	}
	if opts.Full {
		return channels, out, nil
	}

	cursors, err := st.ChannelSyncCursors(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	latestByChannel := make(map[string]store.ChannelSyncCursor, len(cursors))
	for _, cursor := range cursors {
		latestByChannel[cursor.ID] = cursor
	}
	selected := make([]slack.Channel, 0, len(channels))
	source := SourceBot
	if len(sources) > 0 && sources[0] != "" {
		source = sources[0]
	}
	for _, channel := range channels {
		cursor, ok := latestByChannel[channel.ID]
		if !ok {
			floor, err := st.ChannelRetentionFloor(ctx, workspaceID, channel.ID)
			if err != nil {
				return nil, nil, err
			}
			seeded, err := st.ChannelRetentionSeeded(ctx, workspaceID, channel.ID)
			if err != nil {
				return nil, nil, err
			}
			cursor = store.ChannelSyncCursor{ID: channel.ID, RetentionFloor: floor, RetentionSeeded: seeded}
		}
		if opts.LatestOnly && cursor.LatestTS == "" && !cursor.RetentionSeeded {
			continue
		}
		selected = append(selected, channel)
		coverage, err := loadHistoryCoverage(ctx, st, source, workspaceID, channel.ID, "")
		if err != nil {
			return nil, nil, err
		}
		oldest := ""
		if coverage.Pending != nil {
			oldest = *coverage.Pending
		} else if coverage.Complete {
			oldest = repairOldest(coverage.Latest, time.Hour)
		}
		out[channel.ID] = cursor.ApplyRetentionFloor(oldest)
	}
	return selected, out, nil
}

// A saved message is an observation, not evidence that all older pages were
// read. Keep the in-flight interval until the complete request chain succeeds.
type historyCoverage struct {
	Complete bool    `json:"complete"`
	Latest   string  `json:"latest"` // Completed request horizon, including empty history.
	Pending  *string `json:"pending,omitempty"`
}

func historyCoverageKey(workspaceID, channelID, since string) string {
	key, _ := json.Marshal([]string{workspaceID, channelID, since})
	return string(key)
}

func loadHistoryCoverage(ctx context.Context, st *store.Store, source, workspaceID, channelID, since string) (historyCoverage, error) {
	raw, err := st.GetSyncState(ctx, source, "history_coverage_v1", historyCoverageKey(workspaceID, channelID, since))
	if errors.Is(err, sql.ErrNoRows) {
		return historyCoverage{}, nil
	}
	if err != nil {
		return historyCoverage{}, err
	}
	var coverage historyCoverage
	if err := json.Unmarshal([]byte(raw), &coverage); err != nil {
		return historyCoverage{}, fmt.Errorf("invalid API history coverage checkpoint: %w", err)
	}
	return coverage, nil
}

func saveHistoryCoverage(ctx context.Context, st *store.Store, source, workspaceID, channelID, since string, coverage historyCoverage) error {
	raw, err := json.Marshal(coverage)
	if err != nil {
		return err
	}
	return st.SetSyncState(ctx, source, "history_coverage_v1", historyCoverageKey(workspaceID, channelID, since), string(raw))
}

func isChannelHistorySkipped(err error) bool {
	reason := channelSkipReason(err)
	return reason == "not_in_channel" || reason == "channel_not_found"
}

func isThreadRepliesSkipped(err error) bool {
	reason := channelSkipReason(err)
	return reason == "missing_scope" || reason == "not_in_channel" || reason == "channel_not_found"
}

type threadSkipTracker struct {
	mu       sync.Mutex
	scopes   map[string]string
	channels map[string]string
}

func newThreadSkipTracker() *threadSkipTracker {
	return &threadSkipTracker{
		scopes:   make(map[string]string),
		channels: make(map[string]string),
	}
}

func (t *threadSkipTracker) Record(channelID, scope, reason string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if reason == "missing_scope" {
		t.scopes[scope] = reason
		return
	}
	t.channels[channelID] = reason
}

func (t *threadSkipTracker) SkipReason(channelID, scope string) (string, bool) {
	if t == nil {
		return "", false
	}
	t.mu.Lock()
	scopeReason, scopeSkipped := t.scopes[scope]
	channelReason, channelSkipped := t.channels[channelID]
	t.mu.Unlock()
	if scopeSkipped {
		return scopeReason, true
	}
	return channelReason, channelSkipped
}

func (t *threadSkipTracker) Skipped() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	skipped := len(t.scopes) > 0 || len(t.channels) > 0
	t.mu.Unlock()
	return skipped
}

func threadSkipScope(channel slack.Channel) string {
	if kind := dmChannelKind(channel); kind != "" {
		return kind
	}
	if channel.IsPrivate {
		return "private_channel"
	}
	return "public_channel"
}

func channelSkipReason(err error) string {
	var slackErr slack.SlackErrorResponse
	if errors.As(err, &slackErr) && slackErr.Err != "" {
		return slackErr.Err
	}
	return ""
}

func isMissingScopeError(err error) bool {
	return channelSkipReason(err) == "missing_scope"
}

func (c *Client) dmMissingScope(ctx context.Context, workspaceID string) string {
	if !c.dmPolicy.Enabled(c.tokens.User != "") || c.user == nil {
		return ""
	}
	missing := make(map[string]struct{})
	addMissing := func(scope string) {
		if scope == "" {
			return
		}
		missing[scope] = struct{}{}
	}

	dms, err := c.fetchDMs(ctx, workspaceID)
	if err != nil {
		if isMissingScopeError(err) {
			addMissing("im:read")
			addMissing("mpim:read")
		}
		return joinScopes(missing)
	}

	var sampleIM, sampleMPIM string
	for _, dm := range dms {
		switch dmChannelKind(dm) {
		case "im":
			if sampleIM == "" {
				sampleIM = dm.ID
			}
		case "mpim":
			if sampleMPIM == "" {
				sampleMPIM = dm.ID
			}
		}
		if sampleIM != "" && sampleMPIM != "" {
			break
		}
	}

	if sampleIM != "" {
		_, historyErr := c.getConversationHistory(ctx, c.tokens.User, &slack.GetConversationHistoryParameters{
			ChannelID: sampleIM,
			Limit:     1,
		})
		if isMissingScopeError(historyErr) {
			addMissing("im:history")
		}
	}
	if sampleMPIM != "" {
		_, historyErr := c.getConversationHistory(ctx, c.tokens.User, &slack.GetConversationHistoryParameters{
			ChannelID: sampleMPIM,
			Limit:     1,
		})
		if isMissingScopeError(historyErr) {
			addMissing("mpim:history")
		}
	}

	return joinScopes(missing)
}

func joinScopes(scopes map[string]struct{}) string {
	if len(scopes) == 0 {
		return ""
	}
	out := make([]string, 0, len(scopes))
	for scope := range scopes {
		out = append(out, scope)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func (c *Client) userAuthAvailable(ctx context.Context, workspaceID string) (bool, error) {
	if c.user == nil {
		return false, nil
	}
	auth, err := c.authTest(ctx, c.user)
	if err != nil {
		return false, nil
	}
	// Invalid optional auth keeps bot-only coverage; valid auth from another
	// workspace must stop before its replies can be stored under this workspace.
	if _, err := authenticatedWorkspaceID(auth, workspaceID); err != nil {
		return false, fmt.Errorf("user token: %w", err)
	}
	return true, nil
}

func authErrorReason(err error) string {
	if reason := channelSkipReason(err); reason != "" {
		return reason
	}
	return err.Error()
}
