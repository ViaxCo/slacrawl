package share

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
)

func shouldMergeSnapshotRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, state *mergeImportState) (bool, error) {
	type mergeKey struct {
		query     string
		args      []any
		tombstone string
	}
	var key mergeKey
	messageKey := snapshotStringValue(row["channel_id"]) + "\x00" + snapshotStringValue(row["ts"])
	if table == "message_events" {
		if _, rejected := state.retentionRejectedMessages[messageKey]; rejected {
			return false, nil
		}
	}
	if table == "message_files" || table == "message_mentions" {
		if _, rejected := state.rejectedMessages[messageKey]; rejected {
			return false, nil
		}
		allowed, err := subordinateAllowedByParent(ctx, tx, row)
		if err != nil || !allowed {
			return allowed, err
		}
	}
	if table == "channels" || table == "users" || table == "messages" {
		if err := rejectWorkspaceIdentityCollision(ctx, tx, table, row); err != nil {
			return false, err
		}
	}
	switch table {
	case "workspaces":
		key = mergeKey{query: `select updated_at, 0 from workspaces where id = ?`, args: []any{importValue(row["id"])}}
	case "channels":
		key = mergeKey{query: `select updated_at, is_archived from channels where id = ?`, args: []any{importValue(row["id"])}, tombstone: "is_archived"}
	case "users":
		key = mergeKey{query: `select updated_at, is_deleted from users where id = ?`, args: []any{importValue(row["id"])}, tombstone: "is_deleted"}
	case "messages":
		allowed, err := store.MessageAllowedByRetention(ctx, tx, store.Message{
			WorkspaceID: snapshotStringValue(row["workspace_id"]),
			ChannelID:   snapshotStringValue(row["channel_id"]),
			TS:          snapshotStringValue(row["ts"]),
			ThreadTS:    snapshotStringValue(row["thread_ts"]),
		})
		if err != nil {
			return false, fmt.Errorf("apply local retention to snapshot message: %w", err)
		}
		if !allowed {
			state.rejectedMessages[messageKey] = struct{}{}
			state.retentionRejectedMessages[messageKey] = struct{}{}
			return false, nil
		}
		key = mergeKey{query: `select updated_at, trim(coalesce(deleted_ts, '')) <> '' from messages where channel_id = ? and ts = ?`, args: []any{importValue(row["channel_id"]), importValue(row["ts"])}, tombstone: "deleted_ts"}
	case "message_files":
		key = mergeKey{query: `select updated_at, deleted_at is not null from message_files where channel_id = ? and ts = ? and file_id = ?`, args: []any{importValue(row["channel_id"]), importValue(row["ts"]), importValue(row["file_id"])}, tombstone: "deleted_at"}
	case "message_mentions":
		key = mergeKey{query: `select updated_at, deleted_at is not null from message_mentions where channel_id = ? and ts = ? and mention_type = ? and target_id = ?`, args: []any{importValue(row["channel_id"]), importValue(row["ts"]), importValue(row["mention_type"]), importValue(row["target_id"])}, tombstone: "deleted_at"}
	default:
		return true, nil
	}
	var localUpdated string
	var localTombstone bool
	err := tx.QueryRowContext(ctx, key.query, key.args...).Scan(&localUpdated, &localTombstone)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read existing %s merge version: %w", table, err)
	}
	incomingUpdated := snapshotStringValue(row["updated_at"])
	comparison := compareSnapshotTimes(incomingUpdated, localUpdated)
	if comparison != 0 {
		apply := comparison > 0
		if table == "messages" && !apply {
			state.rejectedMessages[messageKey] = struct{}{}
		}
		return apply, nil
	}
	incomingTombstone := false
	if key.tombstone != "" {
		incomingTombstone = snapshotTombstoneValue(row[key.tombstone])
	}
	if table == "messages" && !localTombstone {
		var localSourceRank int64
		var localRawJSON string
		if err := tx.QueryRowContext(ctx, `select source_rank, raw_json from messages where channel_id = ? and ts = ?`, importValue(row["channel_id"]), importValue(row["ts"])).Scan(&localSourceRank, &localRawJSON); err != nil {
			return false, fmt.Errorf("read existing message source rank: %w", err)
		}
		incomingSourceRank := snapshotInt64Value(row["source_rank"])
		apply := incomingSourceRank <= localSourceRank
		if !incomingTombstone {
			apply = incomingSourceRank < localSourceRank || (incomingSourceRank == localSourceRank && snapshotStringValue(row["raw_json"]) == localRawJSON)
		}
		if !apply {
			state.rejectedMessages[messageKey] = struct{}{}
		}
		return apply, nil
	}
	apply := !localTombstone || incomingTombstone
	if table == "messages" && !apply {
		state.rejectedMessages[messageKey] = struct{}{}
	}
	return apply, nil
}

func snapshotInt64Value(value any) int64 {
	switch typed := value.(type) {
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	case int64:
		return typed
	case float64:
		return int64(typed)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}

func rejectWorkspaceIdentityCollision(ctx context.Context, tx *sql.Tx, table string, row map[string]any) error {
	incomingWorkspace := snapshotStringValue(row["workspace_id"])
	var existingWorkspace string
	var id string
	var err error
	if table == "messages" {
		id = messageSnapshotKey(row)
		err = tx.QueryRowContext(ctx, `select workspace_id from messages where channel_id = ? and ts = ?`, importValue(row["channel_id"]), importValue(row["ts"])).Scan(&existingWorkspace)
	} else {
		id = snapshotStringValue(row["id"])
		err = tx.QueryRowContext(ctx, "select workspace_id from "+quoteIdent(table)+" where id = ?", id).Scan(&existingWorkspace) //nolint:gosec // Fixed table allowlist above.
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read existing %s workspace: %w", table, err)
	}
	if existingWorkspace != incomingWorkspace {
		return fmt.Errorf("merge %s %q workspace collision: existing %q, incoming %q", table, id, existingWorkspace, incomingWorkspace)
	}
	return nil
}

func messageSnapshotKey(row map[string]any) string {
	return snapshotStringValue(row["channel_id"]) + "|" + snapshotStringValue(row["ts"])
}

func subordinateAllowedByParent(ctx context.Context, tx *sql.Tx, row map[string]any) (bool, error) {
	channelID := snapshotStringValue(row["channel_id"])
	ts := snapshotStringValue(row["ts"])
	var parentUpdated string
	var parentDeleted bool
	err := tx.QueryRowContext(ctx, `
select updated_at, trim(coalesce(deleted_ts, '')) <> ''
from messages
where channel_id = ? and ts = ?
`, channelID, ts).Scan(&parentUpdated, &parentDeleted)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot subordinate parent: %w", err)
	}
	if !parentDeleted {
		return true, nil
	}
	comparison := compareSnapshotTimes(snapshotStringValue(row["updated_at"]), parentUpdated)
	return comparison > 0 || (comparison == 0 && snapshotTombstoneValue(row["deleted_at"])), nil
}

func compareSnapshotTimes(left, right string) int {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	if leftErr == nil && rightErr == nil {
		switch {
		case leftTime.Before(rightTime):
			return -1
		case leftTime.After(rightTime):
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
}

func snapshotTombstoneValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case json.Number:
		return typed.String() != "0"
	case float64:
		return typed != 0
	case int64:
		return typed != 0
	case string:
		return strings.TrimSpace(typed) != "" && strings.TrimSpace(typed) != "0"
	default:
		return true
	}
}

func snapshotStringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func withoutString(values []string, remove string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return out
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
