package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

func (s *Store) RebuildSearchIndexes(ctx context.Context) error {
	return rebuildSearchIndexes(ctx, s.db)
}

// RebuildSearchIndexesInTransaction atomically aligns FTS rows with messages
// while preserving the caller's surrounding database transaction.
func RebuildSearchIndexesInTransaction(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return errors.New("search-index rebuild transaction is required")
	}
	return rebuildSearchIndexesInTransaction(ctx, tx)
}

// BackfillDeletedSubordinates stamps deletion metadata onto message_files and
// message_mentions whose parent message carries a deleted_ts. It preserves any
// deletion_reason already recorded — the parent tombstone explains missing
// subordinates, it does not override a more specific reason. This is the single
// owner of the tombstone-propagation SQL; the v6 migration and the share import
// path both call it so the invariant cannot drift between them again.
func BackfillDeletedSubordinates(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return errors.New("tombstone backfill transaction is required")
	}
	if _, err := tx.ExecContext(ctx, backfillDeletedSubordinatesSQL); err != nil {
		return fmt.Errorf("backfill deleted message subordinates: %w", err)
	}
	return nil
}

const backfillDeletedSubordinatesSQL = `
update message_files
set deleted_at = coalesce(nullif(deleted_at, ''), (
      select m.updated_at from messages m
      where m.channel_id = message_files.channel_id and m.ts = message_files.ts
    )),
    deletion_source = coalesce(nullif(deletion_source, ''), (
      select m.source_name from messages m
      where m.channel_id = message_files.channel_id and m.ts = message_files.ts
    )),
    deletion_reason = coalesce(nullif(deletion_reason, ''), 'parent_message_deleted'),
    updated_at = coalesce((
      select m.updated_at from messages m
      where m.channel_id = message_files.channel_id and m.ts = message_files.ts
    ), updated_at)
where exists (
  select 1 from messages m
  where m.channel_id = message_files.channel_id and m.ts = message_files.ts
    and trim(coalesce(m.deleted_ts, '')) <> ''
);

update message_mentions
set deleted_at = coalesce(nullif(deleted_at, ''), (
      select m.updated_at from messages m
      where m.channel_id = message_mentions.channel_id and m.ts = message_mentions.ts
    )),
    deletion_source = coalesce(nullif(deletion_source, ''), (
      select m.source_name from messages m
      where m.channel_id = message_mentions.channel_id and m.ts = message_mentions.ts
    )),
    deletion_reason = coalesce(nullif(deletion_reason, ''), 'parent_message_deleted'),
    updated_at = coalesce((
      select m.updated_at from messages m
      where m.channel_id = message_mentions.channel_id and m.ts = message_mentions.ts
    ), updated_at)
where exists (
  select 1 from messages m
  where m.channel_id = message_mentions.channel_id and m.ts = message_mentions.ts
    and trim(coalesce(m.deleted_ts, '')) <> ''
);
`

func markSearchIndexRebuildPending(ctx context.Context, dbtx storedb.DBTX) error {
	return storedb.New(dbtx).SetSyncState(ctx, storedb.SetSyncStateParams{
		SourceName: searchIndexMaintenanceSource, EntityType: searchIndexMaintenanceEntityType,
		EntityID: searchIndexMaintenanceEntityID, Value: "1", UpdatedAt: formatDBTime(time.Now().UTC()),
	})
}

func repairPendingSearchIndex(ctx context.Context, db *sql.DB) error {
	pending, err := searchIndexRepairPending(ctx, db)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	if err := rebuildSearchIndexes(ctx, db); err != nil {
		return fmt.Errorf("repair pending search index: %w", err)
	}
	return nil
}

func searchIndexRepairPending(ctx context.Context, db *sql.DB) (bool, error) {
	var pending bool
	if err := db.QueryRowContext(ctx, `
select exists (
  select 1 from sync_state
  where source_name = ? and entity_type = ? and entity_id = ?
)
`, searchIndexMaintenanceSource, searchIndexMaintenanceEntityType, searchIndexMaintenanceEntityID).Scan(&pending); err != nil {
		return false, fmt.Errorf("inspect search-index maintenance state: %w", err)
	}
	return pending, nil
}

func searchIndexRowsAligned(ctx context.Context, db *sql.DB) (bool, error) {
	var aligned bool
	if err := db.QueryRowContext(ctx, `
select
  (select count(*) from message_fts) = (select count(*) from messages)
  and not exists (
    select 1
    from message_fts f
    left join messages m
      on m.rowid = f.rowid
     and f.message_key = m.channel_id || '|' || m.ts
    where m.rowid is null
  )
`).Scan(&aligned); err != nil {
		return false, fmt.Errorf("inspect search-index row alignment: %w", err)
	}
	return aligned, nil
}

func rebuildSearchIndexes(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := rebuildSearchIndexesInTransaction(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func rebuildSearchIndexesInTransaction(ctx context.Context, dbtx storedb.DBTX) error {
	if _, err := dbtx.ExecContext(ctx, `delete from message_fts`); err != nil {
		return err
	}
	if _, err := dbtx.ExecContext(ctx, rebuildMessageFTSRowsSQL); err != nil {
		return err
	}
	if _, err := dbtx.ExecContext(ctx, `
delete from sync_state
where source_name = ? and entity_type = ? and entity_id = ?
`, searchIndexMaintenanceSource, searchIndexMaintenanceEntityType, searchIndexMaintenanceEntityID); err != nil {
		return err
	}
	return nil
}
