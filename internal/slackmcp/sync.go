package slackmcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/slack-go/slack"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/search"
	"github.com/openclaw/slacrawl/internal/store"
)

const (
	SourceName          = "mcp"
	SourceRank          = 4
	maxMessageBatchSize = 500
)

var channelIDRE = regexp.MustCompile(`^[CDG][A-Z0-9]+$`)

type Options struct {
	DMPolicy        admission.DMPolicy
	WorkspaceID     string
	Channels        []string
	ExcludeChannels []string
	Since           string
	Full            bool
	LatestOnly      bool
	Logger          *slog.Logger
	HTTPClient      *http.Client
	Config          config.MCPConfig
}

type Summary struct {
	NoEligibleConversations bool   `json:"no_eligible_conversations,omitempty"`
	OmittedDM               int    `json:"omitted_dm,omitempty"`
	WorkspaceID             string `json:"workspace_id,omitempty"`
	Channels                int    `json:"channels"`
	Users                   int    `json:"users"`
	Messages                int    `json:"messages"`
	Replies                 int    `json:"replies"`
}

func Sync(ctx context.Context, st *store.Store, opts Options) (Summary, error) {
	workspaceID := strings.TrimSpace(opts.WorkspaceID)
	if workspaceID == "" {
		return Summary{}, errors.New("workspace ID is required for MCP sync because the connector does not report one")
	}
	client, err := New(ctx, opts.Config, opts.HTTPClient)
	if err != nil {
		return Summary{}, err
	}
	defer func() { _ = client.Close() }()
	tools, err := client.discover(ctx)
	if err != nil {
		return Summary{}, err
	}
	if opts.DMPolicy == admission.Exclude && tools.provider != providerReference {
		return Summary{}, errors.New("include_dms=false requires native conversation type evidence; this text MCP adapter cannot verify it; use --source api or a qualified reference MCP server")
	}
	selection, err := resolveAdmissionChannels(ctx, client, tools, opts)
	if err != nil {
		return Summary{}, err
	}
	channels, omittedDM, err := admitMCPChannels(selection, workspaceID, opts.DMPolicy)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{WorkspaceID: workspaceID, OmittedDM: omittedDM}
	if opts.DMPolicy == admission.Exclude && len(channels) == 0 {
		summary.NoEligibleConversations = true
		return summary, nil
	}
	now := time.Now().UTC()
	if err := st.EnsureWorkspace(ctx, store.Workspace{
		ID: workspaceID, Name: workspaceID,
		RawJSON: store.MarshalRaw(map[string]any{"source": SourceName}), UpdatedAt: now,
	}); err != nil {
		return Summary{}, err
	}

	userCount := 0
	if len(opts.Channels) == 0 {
		users, err := client.users(ctx, tools)
		if err != nil {
			return Summary{}, err
		}
		for _, user := range users {
			if err := st.EnsureUser(ctx, toStoreUser(workspaceID, user, now)); err != nil {
				return Summary{}, persistenceError(err)
			}
		}
		userCount = len(users)
	}

	oldestByChannel, selected, err := syncPlan(ctx, st, workspaceID, channels, opts)
	if err != nil {
		return Summary{}, err
	}
	restoreRequested := opts.Since != "" || opts.Full
	summary.Users = userCount
	for _, channel := range selected {
		enforceRetention, err := syncEnforcesRetention(ctx, st, workspaceID, channel.ID, oldestByChannel[channel.ID], restoreRequested)
		if err != nil {
			return summary, err
		}
		channelResult, err := client.channelMessages(ctx, tools, workspaceID, channel.ID, oldestByChannel[channel.ID])
		if err != nil {
			return summary, fmt.Errorf("read MCP channel: %w", err)
		}
		if channel.Name == "" {
			channel.Name = channelResult.ChannelName
		}
		if channel.Kind == "" {
			channel.Kind = "mcp_channel"
		}
		if err := st.EnsureChannel(ctx, toStoreChannel(workspaceID, channel, now)); err != nil {
			return summary, persistenceError(err)
		}
		summary.Channels++

		threadRoots := map[string]struct{}{}
		batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, min(len(channelResult.Messages), maxMessageBatchSize))}
		for _, message := range channelResult.Messages {
			batch.Messages = append(batch.Messages, toMessageWrite(workspaceID, message, enforceRetention, now))
			if len(batch.Messages) == maxMessageBatchSize {
				result, err := st.ApplyWriteBatch(ctx, batch)
				if err != nil {
					return summary, persistenceError(err)
				}
				summary.Messages += result.MessagesWritten
				batch.Messages = batch.Messages[:0]
			}
			if message.ReplyCount > 0 {
				threadRoots[message.TS] = struct{}{}
			}
		}
		if len(batch.Messages) > 0 {
			result, err := st.ApplyWriteBatch(ctx, batch)
			if err != nil {
				return summary, persistenceError(err)
			}
			summary.Messages += result.MessagesWritten
		}
		if tools.readThread == "" {
			continue
		}
		storedRoots, err := st.ChannelThreadRoots(ctx, workspaceID, channel.ID)
		if err != nil {
			return summary, err
		}
		for _, root := range storedRoots {
			threadRoots[root.TS] = struct{}{}
		}
		orderedRoots := make([]string, 0, len(threadRoots))
		for threadTS := range threadRoots {
			orderedRoots = append(orderedRoots, threadTS)
		}
		sort.Strings(orderedRoots)
		for _, threadTS := range orderedRoots {
			replies, err := syncThread(ctx, st, client, tools, workspaceID, channel.ID, threadTS, enforceRetention, now)
			if err != nil {
				return summary, err
			}
			summary.Replies += replies
		}
	}
	if err := st.SetSyncState(ctx, SourceName, "workspace", workspaceID, now.Format(time.RFC3339)); err != nil {
		return summary, err
	}
	return summary, nil
}

func persistenceError(err error) error {
	// Store collisions carry response-controlled IDs. Keep their rejection
	// actionable here without changing shared store diagnostics or transactions.
	if store.IsWorkspaceCollision(err, "") {
		return errors.New("store MCP data: workspace identity conflict; check the configured workspace and archive")
	}
	return err
}

func syncEnforcesRetention(ctx context.Context, st *store.Store, workspaceID, channelID, oldest string, restoreRequested bool) (bool, error) {
	if !restoreRequested {
		return true, nil
	}
	if oldest == "" {
		return false, nil
	}
	floor, err := st.ChannelRetentionFloor(ctx, workspaceID, channelID)
	if err != nil {
		return false, err
	}
	return store.ShouldEnforceRetention(oldest, floor, true), nil
}

func syncThread(ctx context.Context, st *store.Store, client *Client, tools toolset, workspaceID, channelID, threadTS string, enforceRetention bool, now time.Time) (int, error) {
	thread, err := client.threadMessages(ctx, tools, workspaceID, channelID, threadTS)
	if err != nil {
		return 0, fmt.Errorf("read MCP thread: %w", err)
	}
	if thread.Parent != nil && (len(thread.Replies) > 0 || thread.Parent.ReplyCount > 0 || strings.TrimSpace(thread.Parent.LatestReply) != "") {
		thread.Parent.ReplyCount = max(thread.Parent.ReplyCount, len(thread.Replies))
		thread.Parent.LatestReply = latestReplyTS(thread.Parent.LatestReply, thread.Replies)
		if _, err := st.ApplyWriteBatch(ctx, store.WriteBatch{Messages: []store.MessageWrite{
			toMessageWrite(workspaceID, *thread.Parent, enforceRetention, now),
		}}); err != nil {
			return 0, persistenceError(err)
		}
	}
	batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, len(thread.Replies))}
	for _, reply := range thread.Replies {
		batch.Messages = append(batch.Messages, toMessageWrite(workspaceID, reply, enforceRetention, now))
	}
	if len(batch.Messages) == 0 {
		return 0, nil
	}
	result, err := st.ApplyWriteBatch(ctx, batch)
	if err != nil {
		return 0, persistenceError(err)
	}
	return result.MessagesWritten, nil
}

func syncPlan(ctx context.Context, st *store.Store, workspaceID string, channels []ChannelRecord, opts Options) (map[string]string, []ChannelRecord, error) {
	oldest := make(map[string]string, len(channels))
	if opts.Since != "" {
		since := normalizeTimestamp(opts.Since)
		for _, channel := range channels {
			oldest[channel.ID] = since
		}
		return oldest, channels, nil
	}
	if opts.Full {
		return oldest, channels, nil
	}
	cursors, err := st.ChannelSyncCursors(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	latest := make(map[string]store.ChannelSyncCursor, len(cursors))
	for _, cursor := range cursors {
		latest[cursor.ID] = cursor
	}
	selected := make([]ChannelRecord, 0, len(channels))
	for _, channel := range channels {
		cursor, ok := latest[channel.ID]
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
		channelOldest := cursor.ApplyRetentionFloor(overlapTimestamp(cursor.LatestTS, time.Hour))
		if channelOldest != "" && channelOldest == cursor.RetentionFloor {
			channelOldest = previousMicrosecondTimestamp(channelOldest)
		}
		oldest[channel.ID] = channelOldest
	}
	return oldest, selected, nil
}

func toStoreChannel(workspaceID string, channel ChannelRecord, now time.Time) store.Channel {
	return store.Channel{
		ID:          channel.ID,
		WorkspaceID: workspaceID,
		Name:        channel.Name,
		Kind:        channel.Kind,
		Topic:       channel.Topic,
		Purpose:     channel.Purpose,
		IsPrivate:   channel.IsPrivate,
		IsArchived:  channel.IsArchived,
		RawJSON:     store.MarshalRaw(channel),
		UpdatedAt:   now,
	}
}

func toStoreUser(workspaceID string, user UserRecord, now time.Time) store.User {
	return store.User{
		ID:          user.ID,
		WorkspaceID: workspaceID,
		Name:        user.Name,
		RealName:    user.RealName,
		DisplayName: user.Name,
		Title:       user.Title,
		IsBot:       user.IsBot,
		RawJSON:     store.MarshalRaw(user),
		UpdatedAt:   now,
	}
}

func toMessageWrite(workspaceID string, message MessageRecord, enforceRetention bool, now time.Time) store.MessageWrite {
	threadTS := message.ThreadTS
	if threadTS == message.TS {
		threadTS = ""
	}
	slackMessage := slack.Message{Msg: slack.Msg{
		Channel:         message.ChannelID,
		Timestamp:       message.TS,
		ThreadTimestamp: threadTS,
		User:            message.AuthorID,
		Text:            message.Text,
		ReplyCount:      message.ReplyCount,
		LatestReply:     normalizeTimestamp(message.LatestReply),
	}}
	mentions := search.ExtractMentions(message.Text)
	storedMentions := make([]store.Mention, 0, len(mentions))
	for _, mention := range mentions {
		storedMentions = append(storedMentions, store.Mention{
			Type:        mention.Type,
			TargetID:    mention.TargetID,
			DisplayText: mention.DisplayText,
		})
	}
	stored := store.Message{
		ChannelID:      message.ChannelID,
		TS:             message.TS,
		WorkspaceID:    workspaceID,
		UserID:         message.AuthorID,
		ThreadTS:       threadTS,
		Text:           message.Text,
		NormalizedText: search.NormalizeMessage(slackMessage),
		ReplyCount:     message.ReplyCount,
		LatestReply:    normalizeTimestamp(message.LatestReply),
		SourceRank:     SourceRank,
		SourceName:     SourceName,
		RawJSON:        store.MarshalRaw(message),
		UpdatedAt:      now,
		Files:          nil,
	}
	return store.MessageWrite{
		Message:                stored,
		Mentions:               storedMentions,
		PreserveHigherPriority: true,
		EnforceRetention:       enforceRetention,
	}
}

func latestReplyTS(current string, replies []MessageRecord) string {
	latest := normalizeTimestamp(current)
	for _, reply := range replies {
		if reply.TS > latest {
			latest = reply.TS
		}
	}
	return latest
}

func normalizeTimestamp(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return fmt.Sprintf("%d.%06d", parsed.Unix(), parsed.Nanosecond()/1000)
	}
	return value
}

func previousMicrosecondTimestamp(value string) string {
	parts := strings.SplitN(strings.TrimSpace(value), ".", 2)
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return value
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 6 {
		fraction = fraction[:6]
	}
	fraction += strings.Repeat("0", 6-len(fraction))
	microseconds, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return value
	}
	if microseconds == 0 {
		seconds--
		microseconds = 999999
	} else {
		microseconds--
	}
	return fmt.Sprintf("%d.%06d", seconds, microseconds)
}

func overlapTimestamp(value string, overlap time.Duration) string {
	if value == "" {
		return ""
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value
	}
	return strconv.FormatFloat(math.Max(parsed-overlap.Seconds(), 0), 'f', 6, 64)
}
