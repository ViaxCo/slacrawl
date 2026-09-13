package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

const ThreadPendingEntityType = "thread_pending_v1"

// ThreadWork is local replies work, independent of the history source/horizon.
// Generation prevents an older completion from deleting a renewed job.
type ThreadWork struct {
	SourceName  string
	WorkspaceID string
	ChannelID   string
	TS          string
	Generation  string
}

// ThreadWorkDiscovery scopes page-commit discovery to one admitted channel.
// ExcludedTS keeps known or already attempted work on its existing generation.
type ThreadWorkDiscovery struct {
	SourceName  string
	WorkspaceID string
	ChannelID   string
	ExcludedTS  map[string]struct{}
}

func discoverThreadWork(ctx context.Context, q storedb.DBTX, discovery ThreadWorkDiscovery, messageTSs []string) ([]ThreadWork, error) {
	if err := validateThreadWorkSource(discovery.SourceName); err != nil {
		return nil, err
	}
	owner, err := storedb.New(q).GetChannelWorkspace(ctx, discovery.ChannelID)
	if err != nil {
		return nil, err
	}
	if owner != discovery.WorkspaceID {
		return nil, &WorkspaceCollisionError{Entity: "channel", ID: discovery.ChannelID, ExistingWorkspaceID: owner, WorkspaceID: discovery.WorkspaceID}
	}
	if len(messageTSs) == 0 {
		return nil, nil
	}
	keys, err := json.Marshal(messageTSs)
	if err != nil {
		return nil, err
	}
	// Inspect final retained relationships only for admitted page keys. This
	// covers both page orders without trusting a rejected child's raw fields
	// or rescanning the channel; an arriving root can find an archived child.
	rows, err := q.QueryContext(ctx, `select distinct m.ts from json_each(?) k
join messages a on a.channel_id = ? and a.ts = k.value and a.workspace_id = ?
join messages m on m.channel_id = a.channel_id and m.workspace_id = a.workspace_id
  and m.ts = case when coalesce(a.thread_ts, '') = '' then a.ts else a.thread_ts end
join channels c on c.id = m.channel_id and c.workspace_id = m.workspace_id
where `+threadRootPredicate+` order by m.ts`, string(keys), discovery.ChannelID, discovery.WorkspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var requests []ThreadWork
	for rows.Next() {
		var ts string
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		if _, excluded := discovery.ExcludedTS[ts]; !excluded {
			requests = append(requests, ThreadWork{SourceName: discovery.SourceName, WorkspaceID: discovery.WorkspaceID, ChannelID: discovery.ChannelID, TS: ts})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return requests, nil
}

func threadWorkKey(work ThreadWork) string {
	key, _ := json.Marshal([]string{work.WorkspaceID, work.ChannelID, work.TS})
	return string(key)
}

func validateThreadWorkSource(source string) error {
	if source != "api-user" && source != "mcp" {
		return errors.New("unsupported pending thread source")
	}
	return nil
}

// PrepareThreadWork preserves both saved jobs and retained reply hints before
// history can overwrite them. Discovery and renewal share one writer snapshot.
func (s *Store) PrepareThreadWork(ctx context.Context, source, workspaceID, channelID string) ([]ThreadWork, error) {
	if err := validateThreadWorkSource(source); err != nil {
		return nil, err
	}
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return nil, err
	}
	defer rollback()
	owner, err := storedb.New(dbtx).GetChannelWorkspace(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if owner != workspaceID {
		return nil, &WorkspaceCollisionError{Entity: "channel", ID: channelID, ExistingWorkspaceID: owner, WorkspaceID: workspaceID}
	}
	pending, err := pendingThreadWork(ctx, dbtx, source, workspaceID, channelID)
	if err != nil {
		return nil, err
	}
	byTS := make(map[string]ThreadWork, len(pending))
	for _, work := range pending {
		var owner, threadTS, deletedTS, subtype string
		err := dbtx.QueryRowContext(ctx, `select workspace_id, coalesce(thread_ts, ''), coalesce(deleted_ts, ''), coalesce(subtype, '')
from messages where channel_id = ? and ts = ?`, channelID, work.TS).Scan(&owner, &threadTS, &deletedTS, &subtype)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (owner != workspaceID || (threadTS != "" && threadTS != work.TS))) {
			return nil, errors.New("pending thread parent is missing or inconsistent; repair the archive before retrying")
		}
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(deletedTS) != "" || subtype == "message_deleted" {
			if err := retireDeletedThreadWork(ctx, dbtx, workspaceID, channelID, work.TS); err != nil {
				return nil, err
			}
			continue
		}
		byTS[work.TS] = work
	}
	// A sliced sync can leave a skip without a job, then a share merge can
	// tombstone its parent. Validate pending parents first so reconciliation
	// cannot hide malformed work; only selected, owned tombstones qualify.
	rows, err := dbtx.QueryContext(ctx, `select m.ts from messages m
where m.workspace_id = ? and m.channel_id = ? and coalesce(m.thread_ts, '') in ('', m.ts)
  and (trim(coalesce(m.deleted_ts, '')) <> '' or m.subtype = 'message_deleted')
  and exists (select 1 from sync_state s where s.source_name = 'api-user' and s.entity_type = 'thread_skip'
    and s.entity_id = m.workspace_id || '|' || m.channel_id || '|' || m.ts)`, workspaceID, channelID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tombstones []string
	for rows.Next() {
		var ts string
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		tombstones = append(tombstones, ts)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, ts := range tombstones {
		if err := retireThreadWork(ctx, dbtx, workspaceID, channelID, ts); err != nil {
			return nil, err
		}
	}
	roots, err := channelThreadRoots(ctx, dbtx, workspaceID, channelID)
	if err != nil {
		return nil, err
	}
	for _, root := range roots {
		byTS[root.TS] = ThreadWork{SourceName: source, WorkspaceID: workspaceID, ChannelID: channelID, TS: root.TS}
	}
	requests := make([]ThreadWork, 0, len(byTS))
	for _, work := range byTS {
		requests = append(requests, work)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].TS < requests[j].TS })
	queued, err := enqueueThreadWork(ctx, dbtx, requests)
	if err != nil {
		return nil, err
	}
	if err := commit(); err != nil {
		return nil, err
	}
	return queued, nil
}

func (s *Store) PendingThreadWork(ctx context.Context, source, workspaceID, channelID string) ([]ThreadWork, error) {
	if err := validateThreadWorkSource(source); err != nil {
		return nil, err
	}
	return pendingThreadWork(ctx, s.db, source, workspaceID, channelID)
}

func pendingThreadWork(ctx context.Context, q storedb.DBTX, source, workspaceID, channelID string) ([]ThreadWork, error) {
	// Reporting limits must never truncate the authoritative backlog.
	rows, err := q.QueryContext(ctx, `select entity_id, value from sync_state
where source_name = ? and entity_type = ?
  and json_extract(entity_id, '$[0]') = ? and json_extract(entity_id, '$[1]') = ?
order by entity_id`, source, ThreadPendingEntityType, workspaceID, channelID)
	if err != nil {
		return nil, fmt.Errorf("read pending threads: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var work []ThreadWork
	for rows.Next() {
		var key, generation string
		if err := rows.Scan(&key, &generation); err != nil {
			return nil, err
		}
		var parts []string
		if err := json.Unmarshal([]byte(key), &parts); err != nil || len(parts) != 3 || generation == "" {
			return nil, errors.New("invalid pending thread checkpoint")
		}
		item := ThreadWork{SourceName: source, WorkspaceID: parts[0], ChannelID: parts[1], TS: parts[2], Generation: generation}
		if threadWorkKey(item) != key {
			return nil, errors.New("invalid pending thread checkpoint key")
		}
		work = append(work, item)
	}
	return work, rows.Err()
}

// enqueueThreadWork runs after page writes in their transaction. A hint from
// an earlier duplicate row survives even when the final row omits it, while
// collided, deleted, and retention-rejected absent parents cannot create jobs.
func enqueueThreadWork(ctx context.Context, q storedb.DBTX, requests []ThreadWork) ([]ThreadWork, error) {
	queued := make([]ThreadWork, 0, len(requests))
	seen := make(map[string]bool, len(requests))
	for _, work := range requests {
		if err := validateThreadWorkSource(work.SourceName); err != nil {
			return nil, err
		}
		key := threadWorkKey(work)
		identity := work.SourceName + "\x00" + key
		if seen[identity] {
			continue
		}
		seen[identity] = true
		work.Generation = rand.Text()
		result, err := q.ExecContext(ctx, `insert into sync_state (source_name, entity_type, entity_id, value, updated_at)
select ?, ?, ?, ?, ?
where exists (
  select 1 from messages m join channels c on c.id = m.channel_id and c.workspace_id = m.workspace_id
  where m.workspace_id = ? and m.channel_id = ? and m.ts = ?
    and coalesce(m.thread_ts, '') in ('', m.ts) and trim(coalesce(m.deleted_ts, '')) = '' and coalesce(m.subtype, '') <> 'message_deleted'
)
on conflict(source_name, entity_type, entity_id) do update set value = excluded.value, updated_at = excluded.updated_at`,
			work.SourceName, ThreadPendingEntityType, key, work.Generation, formatDBTime(time.Now().UTC()), work.WorkspaceID, work.ChannelID, work.TS)
		if err != nil {
			return nil, fmt.Errorf("enqueue pending thread: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n != 0 {
			queued = append(queued, work)
		}
	}
	return queued, nil
}

// ThreadWorkCurrent checks the same generation and live ownership used by the
// write guard. HTTP runs outside transactions; its result must be checked again.
func (s *Store) ThreadWorkCurrent(ctx context.Context, work ThreadWork) (bool, error) {
	return threadWorkCurrent(ctx, s.db, work)
}

func threadWorkCurrent(ctx context.Context, q storedb.DBTX, work ThreadWork) (bool, error) {
	if err := validateThreadWorkSource(work.SourceName); err != nil {
		return false, err
	}
	var current bool
	err := q.QueryRowContext(ctx, `select exists (
select 1 from sync_state s
join messages m on m.workspace_id = ? and m.channel_id = ? and m.ts = ?
join channels c on c.id = m.channel_id and c.workspace_id = m.workspace_id
where s.source_name = ? and s.entity_type = ? and s.entity_id = ? and s.value = ? and s.value <> ''
  and coalesce(m.thread_ts, '') in ('', m.ts)
  and trim(coalesce(m.deleted_ts, '')) = '' and coalesce(m.subtype, '') <> 'message_deleted'
)`, work.WorkspaceID, work.ChannelID, work.TS, work.SourceName, ThreadPendingEntityType, threadWorkKey(work), work.Generation).Scan(&current)
	return current, err
}

// Completion and API skip cleanup share a writer snapshot so an old response
// cannot remove a renewed generation or the newer attempt's skip state.
func (s *Store) CompleteThreadWork(ctx context.Context, work ThreadWork, apiSkipKey string) error {
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer rollback()
	current, err := threadWorkCurrent(ctx, dbtx, work)
	if err != nil || !current {
		return err
	}
	if _, err := dbtx.ExecContext(ctx, `delete from sync_state
where source_name = ? and entity_type = ? and entity_id = ? and value = ?`,
		work.SourceName, ThreadPendingEntityType, threadWorkKey(work), work.Generation); err != nil {
		return err
	}
	if work.SourceName == "api-user" && apiSkipKey != "" {
		if _, err := dbtx.ExecContext(ctx, `delete from sync_state where source_name = 'api-user' and entity_type = 'thread_skip' and entity_id = ?`, apiSkipKey); err != nil {
			return err
		}
	}
	return commit()
}

// A deletion cancels both consumers' work only if the retained target really is
// tombstoned in this transaction. Skipped/losing deletions leave it untouched.
func retireDeletedThreadWork(ctx context.Context, q storedb.DBTX, workspaceID, channelID, ts string) error {
	var deleted bool
	if err := q.QueryRowContext(ctx, `select exists (select 1 from messages
where workspace_id = ? and channel_id = ? and ts = ? and (trim(coalesce(deleted_ts, '')) <> '' or subtype = 'message_deleted'))`, workspaceID, channelID, ts).Scan(&deleted); err != nil {
		return err
	}
	if !deleted {
		return nil
	}
	return retireThreadWork(ctx, q, workspaceID, channelID, ts)
}

// Call only after an owned deletion or authoritative tombstone check in the
// same transaction. A matching skip can outlive both consumers' pending jobs.
func retireThreadWork(ctx context.Context, q storedb.DBTX, workspaceID, channelID, ts string) error {
	_, err := q.ExecContext(ctx, `delete from sync_state
where (source_name in ('api-user', 'mcp') and entity_type = ? and entity_id = ?)
   or (source_name = 'api-user' and entity_type = 'thread_skip' and entity_id = ?)`,
		ThreadPendingEntityType, threadWorkKey(ThreadWork{WorkspaceID: workspaceID, ChannelID: channelID, TS: ts}), workspaceID+"|"+channelID+"|"+ts)
	return err
}

// Desktop records can carry only the deletion subtype. Queue lifecycle must
// recognize that stored marker without changing message reconciliation.
func messageDeletesThreadWork(message Message) bool {
	return strings.TrimSpace(message.DeletedTS) != "" || message.Subtype == "message_deleted"
}
