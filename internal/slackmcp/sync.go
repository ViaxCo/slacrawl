package slackmcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	summary.Users = userCount
	var coverage messageCoverage
	for _, channel := range channels {
		work, selected, err := st.BeginMCPHistory(ctx, store.MCPHistoryScope{
			WorkspaceID: workspaceID, ChannelID: channel.ID, Adapter: string(tools.provider), Since: normalizeTimestamp(opts.Since),
		}, store.MCPHistoryOptions{Full: opts.Full, LatestOnly: opts.LatestOnly})
		if err != nil {
			return summary, persistenceError(err)
		}
		if !selected {
			continue
		}
		enforceRetention := work.EnforceRetention
		channelResult, err := client.channelMessages(ctx, tools, workspaceID, channel.ID, work.Oldest)
		if err != nil {
			return summary, fmt.Errorf("read MCP channel: %w", err)
		}
		coverage.include(channelResult.coverage)
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

		ordinaryThreads := opts.Since == "" && tools.readThread != ""
		pendingThreads := map[string]store.ThreadWork{}
		if ordinaryThreads {
			work, err := st.PrepareThreadWork(ctx, SourceName, workspaceID, channel.ID, nil)
			if err != nil {
				return summary, persistenceError(err)
			}
			for _, item := range work {
				pendingThreads[item.TS] = item
			}
		}
		threadRoots := map[string]struct{}{}
		var returnedTS []string
		batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, min(len(channelResult.Messages), maxMessageBatchSize))}
		if ordinaryThreads {
			// Keep admitted child evidence with its history batch even if a later
			// batch fails before retained-root discovery can run.
			batch.ThreadDiscovery = &store.ThreadWorkDiscovery{SourceName: SourceName, WorkspaceID: workspaceID, ChannelID: channel.ID}
		}
		writeHistoryBatch := func() error {
			result, err := st.ApplyWriteBatch(ctx, batch)
			if err != nil {
				return persistenceError(err)
			}
			summary.Messages += result.MessagesWritten
			for _, work := range result.PendingThreads {
				pendingThreads[work.TS] = work
			}
			batch.Messages = batch.Messages[:0]
			batch.PendingThreads = batch.PendingThreads[:0]
			return nil
		}
		for _, message := range channelResult.Messages {
			if !ordinaryThreads && tools.readThread != "" {
				returnedTS = append(returnedTS, message.TS)
			}
			batch.Messages = append(batch.Messages, toMessageWrite(workspaceID, message, enforceRetention, now))
			if message.ReplyCount > 0 {
				threadRoots[message.TS] = struct{}{}
				if ordinaryThreads {
					batch.PendingThreads = append(batch.PendingThreads, store.ThreadWork{SourceName: SourceName, WorkspaceID: workspaceID, ChannelID: channel.ID, TS: message.TS})
				}
			}
			if len(batch.Messages) == maxMessageBatchSize {
				if err := writeHistoryBatch(); err != nil {
					return summary, err
				}
			}
		}
		if len(batch.Messages) > 0 {
			if err := writeHistoryBatch(); err != nil {
				return summary, err
			}
		}
		// History owns its checkpoint independently of reply completion. A later
		// reply/channel failure must not erase an already committed history scan.
		if !channelResult.coverage.more && !channelResult.coverage.limited {
			completed, err := st.CompleteMCPHistory(ctx, work, channelResult.LatestTS)
			if err != nil {
				return summary, persistenceError(err)
			}
			if !completed {
				return summary, errors.New("MCP history attempt was superseded; retry sync to complete current history work")
			}
		}
		if tools.readThread == "" {
			if opts.Since == "" {
				work, err := st.ReconcileThreadWork(ctx, SourceName, workspaceID, channel.ID)
				if err != nil {
					return summary, err
				}
				if len(work) > 0 {
					return summary, errors.New("MCP replies work is pending but the connector does not provide a read-thread tool; configure a connector with thread support and retry")
				}
			}
			continue
		}
		if ordinaryThreads {
			// Discover children retained by other writers without replacing an
			// existing generation. Since reads only returned history roots.
			storedRoots, err := st.ChannelThreadRoots(ctx, workspaceID, channel.ID)
			if err != nil {
				return summary, err
			}
			for _, root := range storedRoots {
				batch.PendingThreads = append(batch.PendingThreads, store.ThreadWork{SourceName: SourceName, WorkspaceID: workspaceID, ChannelID: channel.ID, TS: root.TS})
			}
			if len(batch.PendingThreads) > 0 {
				if err := writeHistoryBatch(); err != nil {
					return summary, err
				}
			}
			threadRoots = make(map[string]struct{}, len(pendingThreads))
			for ts := range pendingThreads {
				threadRoots[ts] = struct{}{}
			}
		} else {
			hints := make([]string, 0, len(threadRoots))
			for ts := range threadRoots {
				hints = append(hints, ts)
			}
			roots, err := st.ReturnedThreadRoots(ctx, workspaceID, channel.ID, returnedTS, hints)
			if err != nil {
				return summary, persistenceError(err)
			}
			threadRoots = make(map[string]struct{}, len(roots))
			for _, root := range roots {
				threadRoots[root.TS] = struct{}{}
			}
		}
		orderedRoots := make([]string, 0, len(threadRoots))
		for threadTS := range threadRoots {
			orderedRoots = append(orderedRoots, threadTS)
		}
		sort.Strings(orderedRoots)
		for _, threadTS := range orderedRoots {
			var work *store.ThreadWork
			if pending, ok := pendingThreads[threadTS]; ok {
				work = &pending
			}
			result, err := syncThread(ctx, st, client, tools, workspaceID, channel.ID, threadTS, enforceRetention, now, work)
			if err != nil {
				return summary, err
			}
			if result.revoked {
				continue
			}
			summary.Replies += result.replies
			coverage.include(result.coverage)
			if work != nil && !result.coverage.more && !result.coverage.limited {
				if _, err := st.CompleteThreadWork(ctx, *work, "", nil); err != nil {
					return summary, err
				}
			}
		}
	}
	// Keep valid batches and concrete errors, but never let a later successful
	// response erase an earlier incomplete response before recording freshness.
	if coverage.limited {
		return summary, errors.New("native MCP coverage is incomplete because of Slack history/message limits; received messages were processed without advancing successful sync state; review Slack workspace history availability")
	}
	if coverage.more {
		return summary, errors.New("native MCP history or replies are incomplete; received messages were processed without advancing successful sync state; use --source api for paginated backfill")
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

type threadSyncResult struct {
	replies  int
	coverage messageCoverage
	revoked  bool
}

func syncThread(ctx context.Context, st *store.Store, client *Client, tools toolset, workspaceID, channelID, threadTS string, enforceRetention bool, now time.Time, work *store.ThreadWork) (threadSyncResult, error) {
	var current func() (bool, error)
	if work != nil {
		current = func() (bool, error) { return st.ThreadWorkCurrent(ctx, *work) }
	}
	thread, err := client.threadMessages(ctx, tools, workspaceID, channelID, threadTS, current)
	if current != nil {
		ok, checkErr := current()
		if checkErr != nil {
			return threadSyncResult{}, checkErr
		}
		if !ok {
			return threadSyncResult{revoked: true}, nil
		}
	}
	if thread.revoked {
		return threadSyncResult{revoked: true}, nil
	}
	if err != nil {
		return threadSyncResult{}, fmt.Errorf("read MCP thread: %w", err)
	}
	result := threadSyncResult{coverage: thread.coverage}
	if thread.Parent != nil && (len(thread.Replies) > 0 || thread.Parent.ReplyCount > 0 || strings.TrimSpace(thread.Parent.LatestReply) != "") {
		thread.Parent.ReplyCount = max(thread.Parent.ReplyCount, len(thread.Replies))
		thread.Parent.LatestReply = latestReplyTS(thread.Parent.LatestReply, thread.Replies)
		written, err := st.ApplyWriteBatch(ctx, store.WriteBatch{ThreadGuard: work, Messages: []store.MessageWrite{
			toMessageWrite(workspaceID, *thread.Parent, enforceRetention, now),
		}})
		if err != nil {
			return result, persistenceError(err)
		}
		if written.ThreadWorkRevoked {
			return threadSyncResult{revoked: true}, nil
		}
	}
	batch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, len(thread.Replies)), ThreadGuard: work}
	for _, reply := range thread.Replies {
		batch.Messages = append(batch.Messages, toMessageWrite(workspaceID, reply, enforceRetention, now))
	}
	if len(batch.Messages) == 0 && work == nil {
		return result, nil
	}
	written, err := st.ApplyWriteBatch(ctx, batch)
	if err != nil {
		return result, persistenceError(err)
	}
	if written.ThreadWorkRevoked {
		return threadSyncResult{revoked: true}, nil
	}
	result.replies = written.MessagesWritten
	return result, nil
}

func toStoreChannel(workspaceID string, channel ChannelRecord, now time.Time) store.Channel {
	raw := store.MarshalRaw(channel)
	if channel.native != nil {
		raw = string(channel.native.raw)
	}
	return store.Channel{
		ID:          channel.ID,
		WorkspaceID: workspaceID,
		Name:        channel.Name,
		Kind:        channel.Kind,
		Topic:       channel.Topic,
		Purpose:     channel.Purpose,
		IsPrivate:   channel.IsPrivate,
		IsArchived:  channel.IsArchived,
		RawJSON:     raw,
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
