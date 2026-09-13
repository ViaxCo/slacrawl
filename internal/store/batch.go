package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

func (s *Store) ApplyWriteBatch(ctx context.Context, batch WriteBatch) (WriteBatchResult, error) {
	// BEGIN IMMEDIATE keeps the batched exact-state preflight and subsequent
	// sequential writes in one serialized writer snapshot.
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return WriteBatchResult{}, err
	}
	defer rollback()
	if batch.ThreadGuard != nil {
		current, err := threadWorkCurrent(ctx, dbtx, *batch.ThreadGuard)
		if err != nil {
			return WriteBatchResult{}, err
		}
		if !current {
			return WriteBatchResult{ThreadWorkRevoked: true}, nil
		}
	}
	qtx := storedb.New(dbtx)
	for _, workspace := range batch.Workspaces {
		if err := ensureWorkspace(ctx, dbtx, workspace); err != nil {
			return WriteBatchResult{}, err
		}
	}
	for _, channel := range batch.Channels {
		if err := ensureChannel(ctx, dbtx, channel); err != nil {
			return WriteBatchResult{}, err
		}
	}
	for _, user := range batch.Users {
		if err := ensureUser(ctx, dbtx, user); err != nil {
			return WriteBatchResult{}, err
		}
	}
	unchangedMessages, err := unchangedProviderMessages(ctx, dbtx, batch.Messages)
	if err != nil {
		return WriteBatchResult{}, err
	}
	result := WriteBatchResult{}
	deleted := make(map[ThreadWork]struct{})
	for _, write := range batch.Messages {
		if _, unchanged := unchangedMessages[providerMessageKey(write.Message)]; unchanged {
			continue
		}
		written, err := upsertMessageInTransaction(ctx, dbtx, qtx, write.Message, write.Mentions, write.PreserveHigherPriority, write.EnforceRetention)
		if err != nil {
			if write.SkipWorkspaceCollision && IsWorkspaceCollision(err, "message") {
				result.CollisionsSkipped = append(result.CollisionsSkipped, CollisionSkip{ChannelID: write.Message.ChannelID, TS: write.Message.TS, Err: err})
				continue
			}
			return WriteBatchResult{}, err
		}
		if written {
			result.MessagesWritten++
			if messageDeletesThreadWork(write.Message) {
				deleted[ThreadWork{WorkspaceID: write.Message.WorkspaceID, ChannelID: write.Message.ChannelID, TS: write.Message.TS}] = struct{}{}
			}
		}
	}
	// A later ordered write may revive the target. Only final tombstones cancel
	// work; page hints are then saved against the final admitted parent state.
	for key := range deleted {
		if err := retireDeletedThreadWork(ctx, dbtx, key.WorkspaceID, key.ChannelID, key.TS); err != nil {
			return WriteBatchResult{}, err
		}
	}
	result.PendingThreads, err = enqueueThreadWork(ctx, dbtx, batch.PendingThreads)
	if err != nil {
		return WriteBatchResult{}, err
	}
	for _, state := range batch.SyncStates {
		if err := qtx.SetSyncState(ctx, storedb.SetSyncStateParams{
			SourceName: state.SourceName, EntityType: state.EntityType, EntityID: state.EntityID,
			Value: state.Value, UpdatedAt: formatDBTime(time.Now().UTC()),
		}); err != nil {
			return WriteBatchResult{}, err
		}
	}
	if err := commit(); err != nil {
		return WriteBatchResult{}, err
	}
	return result, nil
}

const providerPreflightChunkSize = 499

type providerMessageIdentity struct {
	channelID string
	ts        string
}

type providerMessageState struct {
	workspaceID    string
	userID         string
	subtype        string
	clientMsgID    string
	threadTS       string
	parentUserID   string
	text           string
	normalizedText string
	replyCount     int64
	latestReply    string
	editedTS       string
	deletedTS      string
	sourceRank     int64
	sourceName     string
	rawJSON        string
}

type providerMentionIdentity struct {
	mentionType string
	targetID    string
}

type providerEventHead struct {
	eventType   string
	sourceName  string
	payloadJSON string
}

type providerMessageCandidate struct {
	key      providerMessageIdentity
	message  Message
	mentions []Mention
}

// unchangedProviderMessages batches provider replay checks. Duplicate message
// keys deliberately stay on the sequential write path because an earlier
// write in the same batch may change the state seen by a later write.
func unchangedProviderMessages(ctx context.Context, dbtx storedb.DBTX, writes []MessageWrite) (map[providerMessageIdentity]struct{}, error) {
	keyCounts := make(map[providerMessageIdentity]int, len(writes))
	for _, write := range writes {
		keyCounts[providerMessageKey(write.Message)]++
	}

	candidates := make([]providerMessageCandidate, 0, len(writes))
	for _, write := range writes {
		key := providerMessageKey(write.Message)
		if write.SkipUnchangedProviderRow && write.Message.Files == nil && keyCounts[key] == 1 {
			candidates = append(candidates, providerMessageCandidate{key: key, message: write.Message, mentions: write.Mentions})
		}
	}

	unchanged := make(map[providerMessageIdentity]struct{}, len(candidates))
	for start := 0; start < len(candidates); start += providerPreflightChunkSize {
		end := min(start+providerPreflightChunkSize, len(candidates))
		chunk := candidates[start:end]
		keys := make([]providerMessageIdentity, len(chunk))
		for i := range chunk {
			keys[i] = chunk[i].key
		}

		states, err := loadProviderMessageStates(ctx, dbtx, keys)
		if err != nil {
			return nil, err
		}
		mentions, err := loadProviderMessageMentions(ctx, dbtx, keys)
		if err != nil {
			return nil, err
		}
		heads, err := loadProviderMessageEventHeads(ctx, dbtx, keys)
		if err != nil {
			return nil, err
		}

		for _, candidate := range chunk {
			state, exists := states[candidate.key]
			if !exists || state != providerMessageStateFromMessage(candidate.message) {
				continue
			}
			if !providerMentionsEqual(mentions[candidate.key], candidate.mentions) {
				continue
			}
			expectedHead := providerEventHead{
				eventType:   eventType(candidate.message),
				sourceName:  candidate.message.SourceName,
				payloadJSON: candidate.message.RawJSON,
			}
			if _, ok := heads[candidate.key][expectedHead]; !ok {
				continue
			}
			unchanged[candidate.key] = struct{}{}
		}
	}
	return unchanged, nil
}

func providerMessageKey(message Message) providerMessageIdentity {
	return providerMessageIdentity{channelID: message.ChannelID, ts: message.TS}
}

func providerMessageStateFromMessage(message Message) providerMessageState {
	return providerMessageState{
		workspaceID: message.WorkspaceID, userID: message.UserID, subtype: message.Subtype,
		clientMsgID: message.ClientMsgID, threadTS: message.ThreadTS, parentUserID: message.ParentUserID,
		text: message.Text, normalizedText: message.NormalizedText, replyCount: int64(message.ReplyCount),
		latestReply: message.LatestReply, editedTS: message.EditedTS, deletedTS: message.DeletedTS,
		sourceRank: int64(message.SourceRank), sourceName: message.SourceName, rawJSON: message.RawJSON,
	}
}

func loadProviderMessageStates(ctx context.Context, dbtx storedb.DBTX, keys []providerMessageIdentity) (map[providerMessageIdentity]providerMessageState, error) {
	predicate, args := providerMessageKeyPredicate(keys)
	rows, err := dbtx.QueryContext(ctx, `
select channel_id, ts, workspace_id, coalesce(user_id, ''), coalesce(subtype, ''),
  coalesce(client_msg_id, ''), coalesce(thread_ts, ''), coalesce(parent_user_id, ''),
  text, normalized_text, reply_count, coalesce(latest_reply, ''), coalesce(edited_ts, ''),
  coalesce(deleted_ts, ''), source_rank, source_name, raw_json
from messages where `+predicate, args...)
	if err != nil {
		return nil, fmt.Errorf("read provider message states: %w", err)
	}
	states := make(map[providerMessageIdentity]providerMessageState, len(keys))
	for rows.Next() {
		var key providerMessageIdentity
		var state providerMessageState
		if err := rows.Scan(
			&key.channelID, &key.ts, &state.workspaceID, &state.userID, &state.subtype,
			&state.clientMsgID, &state.threadTS, &state.parentUserID, &state.text,
			&state.normalizedText, &state.replyCount, &state.latestReply, &state.editedTS,
			&state.deletedTS, &state.sourceRank, &state.sourceName, &state.rawJSON,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan provider message state: %w", err)
		}
		states[key] = state
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read provider message states: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close provider message states: %w", err)
	}
	return states, nil
}

func loadProviderMessageMentions(ctx context.Context, dbtx storedb.DBTX, keys []providerMessageIdentity) (map[providerMessageIdentity]map[providerMentionIdentity]string, error) {
	predicate, args := providerMessageKeyPredicate(keys)
	rows, err := dbtx.QueryContext(ctx, `
select channel_id, ts, mention_type, target_id, coalesce(display_text, '')
from message_mentions where `+predicate, args...)
	if err != nil {
		return nil, fmt.Errorf("read provider message mentions: %w", err)
	}
	mentions := make(map[providerMessageIdentity]map[providerMentionIdentity]string)
	for rows.Next() {
		var key providerMessageIdentity
		var mention providerMentionIdentity
		var displayText string
		if err := rows.Scan(&key.channelID, &key.ts, &mention.mentionType, &mention.targetID, &displayText); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan provider message mention: %w", err)
		}
		if mentions[key] == nil {
			mentions[key] = make(map[providerMentionIdentity]string)
		}
		mentions[key][mention] = displayText
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read provider message mentions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close provider message mentions: %w", err)
	}
	return mentions, nil
}

func loadProviderMessageEventHeads(ctx context.Context, dbtx storedb.DBTX, keys []providerMessageIdentity) (map[providerMessageIdentity]map[providerEventHead]struct{}, error) {
	predicate, args := providerMessageKeyPredicate(keys)
	rows, err := dbtx.QueryContext(ctx, `
select channel_id, ts, event_type, source_name, payload_json
from message_event_heads where `+predicate, args...)
	if err != nil {
		return nil, fmt.Errorf("read provider message event heads: %w", err)
	}
	heads := make(map[providerMessageIdentity]map[providerEventHead]struct{})
	for rows.Next() {
		var key providerMessageIdentity
		var head providerEventHead
		if err := rows.Scan(&key.channelID, &key.ts, &head.eventType, &head.sourceName, &head.payloadJSON); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan provider message event head: %w", err)
		}
		if heads[key] == nil {
			heads[key] = make(map[providerEventHead]struct{})
		}
		heads[key][head] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read provider message event heads: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close provider message event heads: %w", err)
	}
	return heads, nil
}

func providerMentionsEqual(actual map[providerMentionIdentity]string, expected []Mention) bool {
	expectedByID := make(map[providerMentionIdentity]string, len(expected))
	for _, mention := range expected {
		expectedByID[providerMentionIdentity{mentionType: mention.Type, targetID: mention.TargetID}] = mention.DisplayText
	}
	if len(actual) != len(expectedByID) {
		return false
	}
	for key, displayText := range expectedByID {
		if actualDisplayText, ok := actual[key]; !ok || actualDisplayText != displayText {
			return false
		}
	}
	return true
}

func providerMessageKeyPredicate(keys []providerMessageIdentity) (string, []any) {
	var predicate strings.Builder
	args := make([]any, 0, len(keys)*2)
	for i, key := range keys {
		if i > 0 {
			predicate.WriteString(" or ")
		}
		predicate.WriteString("(channel_id = ? and ts = ?)")
		args = append(args, key.channelID, key.ts)
	}
	return predicate.String(), args
}
