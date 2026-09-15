package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

func (s *Store) SetSyncState(ctx context.Context, source, entityType, entityID, value string) error {
	return s.q.SetSyncState(ctx, storedb.SetSyncStateParams{
		SourceName: source,
		EntityType: entityType,
		EntityID:   entityID,
		Value:      value,
		UpdatedAt:  formatDBTime(time.Now().UTC()),
	})
}

func (s *Store) DeleteSyncState(ctx context.Context, source, entityType, entityID string) error {
	return s.q.DeleteSyncState(ctx, storedb.DeleteSyncStateParams{
		SourceName: source,
		EntityType: entityType,
		EntityID:   entityID,
	})
}

func (s *Store) DeleteAPIThreadSkipsIfNoPending(ctx context.Context, workspaceID string) error {
	// Full sync must preserve this workspace's newer pending attempt without
	// letting unrelated workspaces prevent cleanup. The SQL uses one snapshot.
	return s.q.DeleteAPIThreadSkipsIfNoPending(ctx, storedb.DeleteAPIThreadSkipsIfNoPendingParams{
		EntityIDLike: workspaceID + "|%",
		WorkspaceID:  workspaceID,
	})
}

func (s *Store) HasSyncStateType(ctx context.Context, source, entityType string) (bool, error) {
	count, err := s.q.CountSyncStateByType(ctx, storedb.CountSyncStateByTypeParams{
		SourceName: source,
		EntityType: entityType,
	})
	return count > 0, err
}

func (s *Store) ChannelSyncCursors(ctx context.Context, workspaceID string) ([]ChannelSyncCursor, error) {
	rows, err := s.q.ChannelSyncCursors(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	out := make([]ChannelSyncCursor, 0, len(rows))
	for _, row := range rows {
		out = append(out, ChannelSyncCursor{
			ID:              row.ID,
			LatestTS:        row.LatestTs,
			RetentionFloor:  row.RetentionFloor,
			RetentionSeeded: row.RetentionSeeded != 0,
		})
	}
	return out, nil
}

func (s *Store) ChannelRetentionFloor(ctx context.Context, workspaceID, channelID string) (string, error) {
	return retentionFloor(ctx, s.db, workspaceID, channelID)
}

func (s *Store) ChannelRetentionSeeded(ctx context.Context, workspaceID, channelID string) (bool, error) {
	var seeded bool
	err := s.db.QueryRowContext(ctx, `
select exists (
  select 1
  from sync_state
  where source_name = ?
    and entity_type = ?
    and entity_id = ?
)
`, retentionFloorSource, retentionSeedEntityType, workspaceID+"|"+channelID).Scan(&seeded)
	return seeded, err
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func retentionFloor(ctx context.Context, q queryRower, workspaceID, channelID string) (string, error) {
	var value string
	err := q.QueryRowContext(ctx, `
select value
from sync_state
where source_name = ?
  and (
    (entity_type = ? and entity_id = ?)
    or (entity_type = ? and entity_id in (?, '*'))
  )
order by cast(value as real) desc
limit 1
`,
		retentionFloorSource,
		retentionFloorEntityType,
		workspaceID+"|"+channelID,
		retentionScopeEntityType,
		workspaceID,
	).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func messageAllowedByRetention(ctx context.Context, q storedb.DBTX, message Message) (bool, error) {
	floor, err := retentionFloor(ctx, q, message.WorkspaceID, message.ChannelID)
	if err != nil {
		return false, err
	}
	retentionTS := strings.TrimSpace(message.ThreadTS)
	if retentionTS == "" {
		retentionTS = message.TS
	}
	if floor == "" || retentionTimestampAtLeast(retentionTS, floor) {
		return true, nil
	}
	var exists bool
	err = q.QueryRowContext(ctx, `
select exists (
  select 1
  from messages
  where workspace_id = ? and channel_id = ? and ts = ?
)
`, message.WorkspaceID, message.ChannelID, message.TS).Scan(&exists)
	return exists, err
}

// MessageAllowedByRetention reports whether a merge may restore a snapshot row
// without crossing an explicit local purge floor.
func MessageAllowedByRetention(ctx context.Context, tx *sql.Tx, message Message) (bool, error) {
	return messageAllowedByRetention(ctx, tx, message)
}

func retentionTimestampAtLeast(value, floor string) bool {
	valueNumber, valueOK := parseRetentionTimestamp(value)
	floorNumber, floorOK := parseRetentionTimestamp(floor)
	if valueOK && floorOK {
		return valueNumber >= floorNumber
	}
	return value >= floor
}

func parseRetentionTimestamp(value string) (float64, bool) {
	value = strings.TrimSpace(value)
	if parsed, err := strconv.ParseFloat(value, 64); err == nil {
		return parsed, true
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return float64(parsed.Unix()) + float64(parsed.Nanosecond())/float64(time.Second), true
	}
	return 0, false
}

func (s *Store) ChannelThreadRoots(ctx context.Context, workspaceID, channelID string) ([]ThreadRoot, error) {
	return channelThreadRoots(ctx, s.db, workspaceID, channelID)
}

const threadRootPredicate = `coalesce(m.thread_ts, '') in ('', m.ts)
  and trim(coalesce(m.deleted_ts, '')) = ''
  and coalesce(m.subtype, '') <> 'message_deleted'
  and (
    m.reply_count > 0
    or exists (
      select 1 from messages r
      where r.workspace_id = m.workspace_id
        and r.channel_id = m.channel_id
        and r.thread_ts = m.ts
        and r.ts <> m.ts
    )
  )`

func channelThreadRoots(ctx context.Context, q storedb.DBTX, workspaceID, channelID string) ([]ThreadRoot, error) {
	rows, err := q.QueryContext(ctx, `
select m.channel_id, m.ts
from messages m
where m.workspace_id = ? and m.channel_id = ? and `+threadRootPredicate+`
order by m.ts
`, workspaceID, channelID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var roots []ThreadRoot
	for rows.Next() {
		var root ThreadRoot
		if err := rows.Scan(&root.ChannelID, &root.TS); err != nil {
			return nil, err
		}
		roots = append(roots, root)
	}
	return roots, rows.Err()
}

func (s *Store) RenameChannel(ctx context.Context, workspaceID, channelID, name string) error {
	return s.q.RenameChannel(ctx, storedb.RenameChannelParams{
		Name:        name,
		UpdatedAt:   formatDBTime(time.Now().UTC()),
		ID:          channelID,
		WorkspaceID: workspaceID,
	})
}

func (s *Store) SetChannelArchived(ctx context.Context, workspaceID, channelID string, archived bool) error {
	return s.q.SetChannelArchived(ctx, storedb.SetChannelArchivedParams{
		IsArchived:  boolInt(archived),
		UpdatedAt:   formatDBTime(time.Now().UTC()),
		ID:          channelID,
		WorkspaceID: workspaceID,
	})
}

func (s *Store) GetSyncState(ctx context.Context, source, entityType, entityID string) (string, error) {
	value, err := s.q.GetSyncState(ctx, storedb.GetSyncStateParams{
		SourceName: source,
		EntityType: entityType,
		EntityID:   entityID,
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

func (s *Store) ListSyncState(ctx context.Context, source, entityType string, limit int) ([]SyncStateRow, error) {
	rows, err := s.q.ListSyncState(ctx, storedb.ListSyncStateParams{
		SourceName: source,
		EntityType: entityType,
		Limit:      int64(RequireLimit(limit)),
	})
	if err != nil {
		return nil, err
	}
	out := make([]SyncStateRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, SyncStateRow{
			SourceName: row.SourceName,
			EntityType: row.EntityType,
			EntityID:   row.EntityID,
			Value:      row.Value,
		})
	}
	return out, nil
}
