package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThreadWorkDiscoveryAndGeneration(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	now := time.Unix(1710000000, 0).UTC()
	seedBatchCatalog(t, st, "T1", "C1", "U1", now)
	seedBatchCatalog(t, st, "T2", "C2", "U2", now)
	for i := 0; i < 61; i++ {
		msg := batchMessage("C1", fmt.Sprintf("17100000%02d.000000", i), "T1", "parent", now)
		msg.ReplyCount = 1
		if i%2 == 0 {
			msg.ThreadTS = msg.TS
		}
		require.NoError(t, st.UpsertMessage(ctx, msg, nil))
	}
	first, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
	require.NoError(t, err)
	require.Len(t, first, 61, "the authoritative backlog has no reporting limit")
	_, err = st.DB().ExecContext(ctx, "update messages set reply_count=0 where channel_id='C1'")
	require.NoError(t, err)
	renewed, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
	require.NoError(t, err)
	require.Len(t, renewed, 61, "pending work survives overwritten hints")
	for i := range first {
		require.Equal(t, first[i].TS, renewed[i].TS)
		require.NotEqual(t, first[i].Generation, renewed[i].Generation)
		completed, err := st.CompleteThreadWork(ctx, first[i], "", nil)
		require.NoError(t, err)
		require.False(t, completed)
	}
	remaining, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
	require.NoError(t, err)
	require.Equal(t, renewed, remaining, "stale completion cannot delete a newer generation")
	other, err := st.PrepareThreadWork(ctx, "api-user", "T2", "C2", nil)
	require.NoError(t, err)
	require.Empty(t, other)
	for _, item := range renewed {
		completed, err := st.CompleteThreadWork(ctx, item, "", nil)
		require.NoError(t, err)
		require.True(t, completed)
	}
	remaining, err = st.PendingThreadWork(ctx, "api-user", "T1", "C1")
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestThreadRootEvidence(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	now := time.Unix(1710000000, 0).UTC()
	seedBatchCatalog(t, st, "T1", "C1", "U1", now)
	for _, tc := range []struct {
		ts, thread, deleted, subtype string
		replies                      int
	}{
		{"1", "", "", "", 1}, {"2", "2", "", "", 1},
		{"3", "3", "", "", 0}, {"4", "", "", "", 0}, {"5", "4", "", "", 0},
		{"6", "", "7", "", 1}, {"7", "1", "", "", 1}, {"8", "", "", "message_deleted", 1},
	} {
		msg := batchMessage("C1", tc.ts, "T1", "fixture", now)
		msg.ThreadTS, msg.DeletedTS, msg.ReplyCount = tc.thread, tc.deleted, tc.replies
		msg.Subtype = tc.subtype
		require.NoError(t, st.UpsertMessage(ctx, msg, nil))
	}
	roots, err := st.ChannelThreadRoots(ctx, "T1", "C1")
	require.NoError(t, err)
	require.Equal(t, []ThreadRoot{{ChannelID: "C1", TS: "1"}, {ChannelID: "C1", TS: "2"}, {ChannelID: "C1", TS: "4"}}, roots)
}

func TestThreadPreparationReconcilesStoredTombstones(t *testing.T) {
	for _, marker := range []string{"deleted_ts", "subtype", "deleted_ts-no-pending", "subtype-no-pending", "skip-rollback"} {
		t.Run(marker, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			msg := batchMessage("C1", "1710000001.000000", "T1", "parent", now)
			msg.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, msg, nil))
			if marker == "deleted_ts" || marker == "subtype" {
				for _, source := range []string{"api-user", "mcp"} {
					_, err := st.PrepareThreadWork(ctx, source, "T1", "C1", nil)
					require.NoError(t, err)
				}
			}
			skipKey := "T1|C1|" + msg.TS
			beforeSkips := seedThreadWorkSkipFixtures(t, st, skipKey)
			// A share merge can write tombstones directly. Preparation must
			// reconcile those stored markers before attempting replies.
			query := "update messages set deleted_ts='1710000002.000000', reply_count=0"
			if marker == "subtype" || marker == "subtype-no-pending" {
				query = "update messages set subtype='message_deleted', reply_count=0"
			}
			_, err := st.DB().ExecContext(ctx, query)
			require.NoError(t, err)
			if marker == "skip-rollback" {
				_, err := st.DB().ExecContext(ctx, `create trigger reject_skip_reconcile before delete on sync_state when old.entity_type='thread_skip' begin select raise(abort,'synthetic_skip_reconcile_failure'); end`)
				require.NoError(t, err)
			}
			queued, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			if marker == "skip-rollback" {
				require.ErrorContains(t, err, "synthetic_skip_reconcile_failure")
				requireThreadWorkSkips(t, st, beforeSkips)
			} else {
				require.NoError(t, err)
				requireThreadWorkSkips(t, st, beforeSkips, skipKey)
			}
			require.Empty(t, queued)
			assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 0)
		})
	}
}

func TestThreadWorkKeysKeepWorkspaceAndChannelSeparate(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	now := time.Unix(1710000000, 0).UTC()
	var keys []string
	for i, parts := range [][2]string{{"T|part", "C"}, {"T", "part|C"}} {
		seedBatchCatalog(t, st, parts[0], parts[1], fmt.Sprintf("U%d", i), now)
		msg := batchMessage(parts[1], "1", parts[0], "parent", now)
		msg.ReplyCount = 1
		require.NoError(t, st.UpsertMessage(ctx, msg, nil))
		work, err := st.PrepareThreadWork(ctx, "api-user", parts[0], parts[1], nil)
		require.NoError(t, err)
		require.Len(t, work, 1)
		keys = append(keys, threadWorkKey(work[0]))
		pending, err := st.PendingThreadWork(ctx, "api-user", parts[0], parts[1])
		require.NoError(t, err)
		require.Equal(t, work, pending)
	}
	require.NotEqual(t, keys[0], keys[1])
	_, err := st.PrepareThreadWork(ctx, "api-user", "T", "C", nil)
	require.True(t, IsWorkspaceCollision(err, "channel"))
}

func TestThreadSkipReconciliationStaysWithinOwnedRoots(t *testing.T) {
	ctx := context.Background()
	st := openBatchTestStore(t)
	now := time.Unix(1710000000, 0).UTC()
	seedBatchCatalog(t, st, "T1", "C1", "U1", now)
	seedBatchCatalog(t, st, "T1", "C2", "U2", now)
	for _, tc := range []struct{ workspace, channel, ts, thread, deleted string }{
		{"T1", "C1", "1", "", "2"},
		{"T1", "C1", "2", "", ""},
		{"T1", "C1", "3", "absent-root", "4"},
		{"TOTHER", "C1", "4", "", "5"},
		{"T1", "C2", "5", "", "6"},
	} {
		msg := batchMessage(tc.channel, tc.ts, tc.workspace, "fixture", now)
		msg.ThreadTS, msg.DeletedTS = tc.thread, tc.deleted
		require.NoError(t, st.UpsertMessage(ctx, msg, nil))
		require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", tc.workspace+"|"+tc.channel+"|"+tc.ts, "old skip"))
	}
	require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|absent", "no deletion evidence"))
	before := threadWorkSkipRows(t, st)
	work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
	require.NoError(t, err)
	require.Empty(t, work)
	requireThreadWorkSkips(t, st, before, "T1|C1|1")
}

func TestThreadWorkPageAtomicity(t *testing.T) {
	for _, mode := range []string{"duplicate-hint", "enqueue-failure", "foreign", "absent", "deleted", "child"} {
		t.Run(mode, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			msg := batchMessage("C1", "1710000001.000000", "T1", "hint", now)
			msg.ReplyCount = 1
			work := ThreadWork{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: msg.TS}
			batch := WriteBatch{Messages: []MessageWrite{{Message: msg}}, PendingThreads: []ThreadWork{work}}
			switch mode {
			case "duplicate-hint":
				msg.ReplyCount, msg.Text = 0, "hint overwritten"
				batch.Messages = append(batch.Messages, MessageWrite{Message: msg})
				batch.PendingThreads = append(batch.PendingThreads, work)
			case "enqueue-failure":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_thread before insert on sync_state when new.entity_type='thread_pending_v1' begin select raise(abort,'synthetic_queue_failure'); end`)
				require.NoError(t, err)
			case "foreign":
				batch.PendingThreads[0].WorkspaceID = "T2"
			case "absent":
				batch.Messages = nil
			case "deleted":
				batch.Messages[0].Message.DeletedTS = "1710000002.000000"
			case "child":
				batch.Messages[0].Message.ThreadTS = "1710000000.000000"
			}
			result, err := st.ApplyWriteBatch(ctx, batch)
			if mode == "enqueue-failure" {
				require.ErrorContains(t, err, "synthetic_queue_failure")
				assertBatchCount(t, st, "select count(*) from messages", 0)
				assertBatchCount(t, st, "select count(*) from message_events", 0)
				assertBatchCount(t, st, "select count(*) from sync_state", 0)
				return
			}
			require.NoError(t, err)
			if mode == "duplicate-hint" {
				require.Len(t, result.PendingThreads, 1)
				assertBatchCount(t, st, "select count(*) from messages where reply_count=0 and text='hint overwritten'", 1)
				pending, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
				require.NoError(t, err)
				require.Equal(t, result.PendingThreads, pending)
			} else {
				require.Empty(t, result.PendingThreads)
				assertBatchCount(t, st, "select count(*) from sync_state", 0)
			}
		})
	}
}

func TestThreadWorkDiscoveryAtPageCommit(t *testing.T) {
	for _, mode := range []string{"root-child", "child-root", "root-page", "child-page", "foreign-child", "priority-child", "retained-child", "overwritten-child", "positive-hint", "pending", "completed", "enqueue-failure", "nil", "invalid-source", "invalid-scope", "unrelated-request"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			child := batchMessage("C1", "1710000002.000000", "T1", "child", now)
			child.ThreadTS = root.TS
			discovery := &ThreadWorkDiscovery{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", ExcludedTS: map[string]struct{}{}}
			batch := WriteBatch{Messages: []MessageWrite{{Message: root}, {Message: child}}, ThreadDiscovery: discovery}
			wantSource := "api-user"
			wantQueued := true
			var before []map[string]any
			switch mode {
			case "child-root":
				batch.Messages[0], batch.Messages[1] = batch.Messages[1], batch.Messages[0]
			case "root-page", "child-page":
				first, second := root, child
				if mode == "child-page" {
					first, second = second, first
				}
				result, err := st.ApplyWriteBatch(ctx, WriteBatch{Messages: []MessageWrite{{Message: first}}, ThreadDiscovery: discovery})
				require.NoError(t, err)
				require.Empty(t, result.PendingThreads, "one unhinted row alone does not establish an owned thread")
				batch.Messages = []MessageWrite{{Message: second}}
			case "foreign-child":
				seedBatchCatalog(t, st, "T2", "C2", "U2", now)
				foreign := child
				foreign.WorkspaceID = "T2"
				require.NoError(t, st.UpsertMessage(ctx, foreign, nil))
				batch.Messages[1].SkipWorkspaceCollision = true
				wantQueued = false
			case "priority-child", "retained-child":
				retained := child
				retained.SourceName, retained.SourceRank = "api-user", 1
				if mode == "priority-child" {
					retained.ThreadTS = ""
					wantQueued = false
				} else {
					batch.Messages[1].Message.ThreadTS = "1710000099.000000"
				}
				require.NoError(t, st.UpsertMessage(ctx, retained, nil))
				batch.Messages[1].PreserveHigherPriority = true
			case "overwritten-child", "positive-hint":
				child.ThreadTS = "1710000099.000000"
				batch.Messages = append(batch.Messages, MessageWrite{Message: child})
				wantQueued = mode == "positive-hint"
				if wantQueued {
					batch.Messages[0].Message.ReplyCount = 1
					batch.Messages = append(batch.Messages, MessageWrite{Message: root})
					batch.PendingThreads = []ThreadWork{{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}}
				}
			case "pending", "completed":
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				require.NoError(t, st.UpsertMessage(ctx, child, nil))
				work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
				require.NoError(t, err)
				require.Len(t, work, 1)
				if mode == "completed" {
					completed, err := st.CompleteThreadWork(ctx, work[0], "", nil)
					require.NoError(t, err)
					require.True(t, completed)
					discovery.ExcludedTS[root.TS] = struct{}{}
				}
				before, err = st.QueryReadOnly(ctx, "select * from sync_state")
				require.NoError(t, err)
				wantQueued = false
			case "enqueue-failure":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_discovered_thread before insert on sync_state when new.entity_type='thread_pending_v1' begin select raise(abort,'synthetic_discovery_queue_failure'); end`)
				require.NoError(t, err)
			case "nil":
				batch.ThreadDiscovery = nil
				wantQueued = false
			case "invalid-source":
				discovery.SourceName = "unsupported"
				batch.Messages = nil
			case "invalid-scope":
				discovery.WorkspaceID = "T2"
				batch.Messages = nil
			case "unrelated-request":
				discovery.ExcludedTS[root.TS] = struct{}{}
				wantSource = "mcp"
				batch.PendingThreads = []ThreadWork{{SourceName: wantSource, WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}}
			}
			result, err := st.ApplyWriteBatch(ctx, batch)
			if mode == "enqueue-failure" || mode == "invalid-source" || mode == "invalid-scope" {
				require.Error(t, err)
				if mode == "enqueue-failure" {
					require.ErrorContains(t, err, "synthetic_discovery_queue_failure")
				} else if mode == "invalid-scope" {
					require.True(t, IsWorkspaceCollision(err, "channel"))
				} else {
					require.ErrorContains(t, err, "unsupported pending thread source")
				}
				for _, table := range []string{"messages", "message_events", "message_event_heads", "message_mentions", "message_files", "message_fts", "sync_state"} {
					assertBatchCount(t, st, "select count(*) from "+table, 0)
				}
				return
			}
			require.NoError(t, err)
			if mode == "foreign-child" {
				require.Len(t, result.CollisionsSkipped, 1)
			}
			if wantQueued {
				require.Len(t, result.PendingThreads, 1)
				work := result.PendingThreads[0]
				require.Equal(t, wantSource, work.SourceName)
				require.Equal(t, "T1", work.WorkspaceID)
				require.Equal(t, "C1", work.ChannelID)
				require.Equal(t, root.TS, work.TS)
				require.NotEmpty(t, work.Generation)
				pending, err := st.PendingThreadWork(ctx, wantSource, "T1", "C1")
				require.NoError(t, err)
				require.Equal(t, result.PendingThreads, pending)
			} else {
				require.Empty(t, result.PendingThreads)
				after, err := st.QueryReadOnly(ctx, "select * from sync_state")
				require.NoError(t, err)
				require.Equal(t, before, after, "discovery cannot renew pending work or recreate completed work")
			}
			requireMessageFTSParity(t, st)
		})
	}
}

func TestThreadWorkPreservesConcurrentGeneration(t *testing.T) {
	for _, evidence := range []string{"hint", "child"} {
		t.Run(evidence, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "concurrent.db")
			st, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			owner, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, owner.Close()) })
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			prepared, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			require.NoError(t, err)
			require.Empty(t, prepared)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.ReplyCount = 1
			child := batchMessage("C1", "1710000002.000000", "T1", "child", now)
			child.ThreadTS = root.TS
			if evidence == "child" {
				root.ReplyCount = 0
				require.NoError(t, owner.UpsertMessage(ctx, child, nil))
			}
			require.NoError(t, owner.UpsertMessage(ctx, root, nil))
			work, err := owner.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			require.NoError(t, err)
			require.Len(t, work, 1)
			skipKey := "T1|C1|" + root.TS
			require.NoError(t, owner.SetSyncState(ctx, "api-user", "thread_skip", skipKey, "owner skip"))
			before, err := owner.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			batch := WriteBatch{Messages: []MessageWrite{{Message: root}}, ThreadDiscovery: &ThreadWorkDiscovery{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1"}}
			if evidence == "hint" {
				batch.PendingThreads = []ThreadWork{{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}}
			} else {
				batch.Messages = append(batch.Messages, MessageWrite{Message: child})
			}
			result, err := st.ApplyWriteBatch(ctx, batch)
			require.NoError(t, err)
			require.Empty(t, result.PendingThreads, "a page must neither replace nor adopt another invocation's generation")
			after, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			require.Equal(t, before, after)
			current, err := owner.ThreadWorkCurrent(ctx, work[0])
			require.NoError(t, err)
			require.True(t, current)
			completed, err := owner.CompleteThreadWork(ctx, work[0], skipKey, nil)
			require.NoError(t, err)
			require.True(t, completed)
			assertBatchCount(t, st, "select count(*) from sync_state", 0)
			requireMessageFTSParity(t, st)
		})
	}
}

func TestThreadWorkRevalidatedAtPageCommit(t *testing.T) {
	for _, mode := range []string{"hint", "duplicate-hint", "child", "current", "renewed", "completed", "revoked-completion", "still-deleted", "enqueue-failure", "read-failure", "nil", "unrelated-source"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.SourceName, root.SourceRank = "api-bot", 2
			root.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, root, nil))
			prepared, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			require.NoError(t, err)
			require.Len(t, prepared, 1)
			work := prepared[0]
			discovery := &ThreadWorkDiscovery{SourceName: work.SourceName, WorkspaceID: work.WorkspaceID, ChannelID: work.ChannelID,
				ExcludedTS: map[string]struct{}{}}
			if mode != "current" && mode != "renewed" && mode != "completed" && mode != "nil" && mode != "unrelated-source" {
				deleted := root
				deleted.DeletedTS = root.TS
				require.NoError(t, st.MarkMessageDeleted(ctx, deleted, nil))
				pending, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
				require.NoError(t, err)
				require.Empty(t, pending)
			}
			if mode == "renewed" {
				_, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
				require.NoError(t, err)
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|"+root.TS, "new attempt skip"))
			}
			if mode == "completed" || mode == "revoked-completion" {
				completed, err := st.CompleteThreadWork(ctx, work, "", nil)
				require.NoError(t, err)
				require.Equal(t, mode == "completed", completed)
				if completed {
					discovery.ExcludedTS[root.TS] = struct{}{}
				}
			}
			root.Text, root.NormalizedText, root.RawJSON = "revived root", "revived root", `{"text":"revived root"}`
			batch := WriteBatch{Messages: []MessageWrite{{Message: root}}, PendingThreads: []ThreadWork{work}, ThreadDiscovery: discovery}
			switch mode {
			case "duplicate-hint":
				root.ReplyCount = 0
				batch.Messages = append(batch.Messages, MessageWrite{Message: root})
			case "child":
				batch.Messages[0].Message.ReplyCount = 0
				child := batchMessage("C1", "1710000002.000000", "T1", "child", now)
				child.SourceName, child.SourceRank = "api-bot", 2
				child.ThreadTS = root.TS
				batch.Messages = append(batch.Messages, MessageWrite{Message: child})
				batch.PendingThreads = nil
			case "still-deleted":
				batch.Messages = nil
			case "nil":
				batch.ThreadDiscovery = nil
			case "unrelated-source":
				batch.PendingThreads[0].SourceName = "mcp"
			case "enqueue-failure":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_requeued_thread before insert on sync_state when new.entity_type='thread_pending_v1' begin select raise(abort,'synthetic_requeue_failure'); end`)
				require.NoError(t, err)
			case "read-failure":
				_, err := st.DB().ExecContext(ctx, "alter table sync_state rename to saved_sync_state")
				require.NoError(t, err)
			}
			stateTable := "sync_state"
			if mode == "read-failure" {
				stateTable = "saved_sync_state"
			}
			tables := []string{"messages", "message_events", "message_event_heads", "message_mentions", "message_files", "message_fts", stateTable}
			before := map[string]any{}
			for _, table := range tables {
				before[table], err = st.QueryReadOnly(ctx, "select * from "+table)
				require.NoError(t, err)
			}
			result, err := st.ApplyWriteBatch(ctx, batch)
			if mode == "enqueue-failure" || mode == "read-failure" {
				require.Error(t, err)
				if mode == "enqueue-failure" {
					require.ErrorContains(t, err, "synthetic_requeue_failure")
				} else {
					require.ErrorContains(t, err, "no such table: sync_state")
				}
				for _, table := range tables {
					after, err := st.QueryReadOnly(ctx, "select * from "+table)
					require.NoError(t, err)
					require.Equal(t, before[table], after, table+" must roll back with requeue failure")
				}
				return
			}
			require.NoError(t, err)
			if mode == "current" || mode == "renewed" || mode == "completed" || mode == "still-deleted" {
				require.Empty(t, result.PendingThreads)
				after, err := st.QueryReadOnly(ctx, "select * from sync_state")
				require.NoError(t, err)
				require.Equal(t, before[stateTable], after, "existing work/skip and completed exclusions remain unchanged")
			} else {
				require.Len(t, result.PendingThreads, 1)
				queued := result.PendingThreads[0]
				require.Equal(t, root.TS, queued.TS)
				require.NotEmpty(t, queued.Generation)
				require.NotEqual(t, work.Generation, queued.Generation)
				current, err := st.ThreadWorkCurrent(ctx, queued)
				require.NoError(t, err)
				require.True(t, current)
				current, err = st.ThreadWorkCurrent(ctx, work)
				require.NoError(t, err)
				require.Equal(t, mode == "unrelated-source", current, "an old canceled generation cannot become current again")
				if mode == "unrelated-source" {
					require.Equal(t, "mcp", queued.SourceName)
					pending, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
					require.NoError(t, err)
					require.Equal(t, prepared, pending)
				}
			}
			requireMessageFTSParity(t, st)
		})
	}
}

func TestThreadWorkRejectsInconsistentParents(t *testing.T) {
	for _, mode := range []string{"missing", "foreign", "child", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			work := ThreadWork{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: "1710000001.000000"}
			require.NoError(t, st.SetSyncState(ctx, work.SourceName, ThreadPendingEntityType, threadWorkKey(work), "unchanged"))
			if mode == "foreign" || mode == "child" {
				msg := batchMessage("C1", work.TS, "T1", "fixture", now)
				if mode == "foreign" {
					msg.WorkspaceID = "T2"
				} else {
					msg.ThreadTS = "1710000000.000000"
				}
				require.NoError(t, st.UpsertMessage(ctx, msg, nil))
			}
			deleted := batchMessage("C1", "1710000010.000000", "T1", "stored tombstone", now)
			deleted.Subtype = "message_deleted"
			require.NoError(t, st.UpsertMessage(ctx, deleted, nil))
			skipKey := "T1|C1|" + deleted.TS
			beforeSkips := seedThreadWorkSkipFixtures(t, st, skipKey)
			if mode == "canceled" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			_, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorContains(t, err, "pending thread parent is missing or inconsistent")
			}
			value, err := st.GetSyncState(context.Background(), work.SourceName, ThreadPendingEntityType, threadWorkKey(work))
			require.NoError(t, err)
			require.Equal(t, "unchanged", value)
			requireThreadWorkSkips(t, st, beforeSkips)
		})
	}
}

func TestThreadWorkDeletionLifecycle(t *testing.T) {
	for _, mode := range []string{"batch", "standalone", "standalone-no-pending", "losing", "resurrected", "collision", "rollback", "skip-rollback", "retention-skipped", "subtype-batch", "subtype-standalone"} {
		t.Run(mode, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			msg := batchMessage("C1", "1710000001.000000", "T1", "parent", now)
			msg.ReplyCount = 1
			msg.Files = []MessageFile{{FileID: "F1", Name: "parent-file", RawJSON: "{}"}}
			require.NoError(t, st.UpsertMessage(ctx, msg, []Mention{{Type: "user", TargetID: "U1"}}))
			deletion := msg
			deletion.DeletedTS, deletion.UpdatedAt = "1710000002.000000", now.Add(time.Second)
			deletion.Files = nil
			if mode == "subtype-batch" || mode == "subtype-standalone" {
				deletion.DeletedTS, deletion.Subtype = "", "message_deleted"
			}
			key := threadWorkKey(ThreadWork{WorkspaceID: "T1", ChannelID: "C1", TS: msg.TS})
			for _, source := range []string{"api-user", "mcp", "other"} {
				if mode == "standalone-no-pending" && source != "other" {
					continue
				}
				require.NoError(t, st.SetSyncState(ctx, source, ThreadPendingEntityType, key, "pending"))
			}
			write := MessageWrite{Message: deletion}
			batch := WriteBatch{Messages: []MessageWrite{write}}
			switch mode {
			case "losing":
				batch.Messages[0].Message.SourceRank = 6
				batch.Messages[0].PreserveHigherPriority = true
			case "resurrected":
				msg.UpdatedAt = now.Add(2 * time.Second)
				batch.Messages = append(batch.Messages, MessageWrite{Message: msg})
			case "collision":
				batch.Messages[0].Message.WorkspaceID = "T2"
				batch.Messages[0].SkipWorkspaceCollision = true
			case "rollback":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_thread_delete before delete on sync_state when old.entity_type='thread_pending_v1' begin select raise(abort,'synthetic_retire_failure'); end`)
				require.NoError(t, err)
			case "skip-rollback":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_skip_delete before delete on sync_state when old.source_name='api-user' and old.entity_type='thread_skip' begin select raise(abort,'synthetic_skip_retire_failure'); end`)
				require.NoError(t, err)
			case "retention-skipped":
				opts := PurgeOptions{Before: now.Add(10 * time.Second), Delete: true, WorkspaceID: "T1"}
				_, err := st.PurgeMessages(ctx, opts)
				require.NoError(t, err)
				for _, source := range []string{"api-user", "mcp"} {
					require.NoError(t, st.SetSyncState(ctx, source, ThreadPendingEntityType, key, "pending"))
				}
				batch.Messages[0].EnforceRetention = true
			}
			// Seed after the setup purge: a skipped deletion must preserve a
			// skip created later, independent of whether a pending row exists.
			skipKey := "T1|C1|" + msg.TS
			beforeSkips := seedThreadWorkSkipFixtures(t, st, skipKey)
			beforeRollback := map[string][]map[string]any{}
			if mode == "rollback" || mode == "skip-rollback" {
				for _, table := range []string{"messages", "message_events", "message_event_heads", "message_files", "message_mentions", "message_fts", "sync_state"} {
					rows, err := st.QueryReadOnly(ctx, "select * from "+table)
					require.NoError(t, err)
					beforeRollback[table] = rows
				}
			}
			var err error
			if mode == "standalone" || mode == "standalone-no-pending" {
				err = st.MarkMessageDeleted(ctx, deletion, nil)
			} else if mode == "subtype-standalone" {
				err = st.UpsertMessage(ctx, deletion, nil)
			} else {
				_, err = st.ApplyWriteBatch(ctx, batch)
			}
			if mode == "rollback" {
				require.ErrorContains(t, err, "synthetic_retire_failure")
			} else if mode == "skip-rollback" {
				require.ErrorContains(t, err, "synthetic_skip_retire_failure")
			} else {
				require.NoError(t, err)
			}
			wins := mode == "batch" || mode == "standalone" || mode == "standalone-no-pending" || mode == "subtype-batch" || mode == "subtype-standalone"
			if wins {
				requireThreadWorkSkips(t, st, beforeSkips, skipKey)
			} else {
				requireThreadWorkSkips(t, st, beforeSkips)
			}
			for table, before := range beforeRollback {
				after, err := st.QueryReadOnly(ctx, "select * from "+table)
				require.NoError(t, err)
				require.Equal(t, before, after, table)
			}
			assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1' and source_name in ('api-user','mcp')", map[bool]int64{true: 0, false: 2}[wins])
			assertBatchCount(t, st, "select count(*) from sync_state where source_name='other'", 1)
			if mode != "retention-skipped" {
				assertBatchCount(t, st, "select count(*) from messages where trim(coalesce(deleted_ts,''))<>'' or subtype='message_deleted'", map[bool]int64{true: 1, false: 0}[wins])
				assertBatchCount(t, st, "select count(*) from message_files where deleted_at is null", map[bool]int64{true: 0, false: 1}[mode == "standalone" || mode == "standalone-no-pending"])
				requireMessageFTSParity(t, st)
			}
		})
	}
}

func TestThreadWorkPurgeAndFreshness(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	now := time.Unix(1710000000, 0).UTC()
	seedBatchCatalog(t, st, "T1", "C1", "U1", now)
	seedBatchCatalog(t, st, "T2", "C2", "U2", now)
	for _, channel := range []string{"C1", "C2"} {
		workspace := map[string]string{"C1": "T1", "C2": "T2"}[channel]
		msg := batchMessage(channel, "1710000001.000000", workspace, "parent", now)
		msg.ReplyCount = 1
		require.NoError(t, st.UpsertMessage(ctx, msg, nil))
		for _, source := range []string{"api-user", "mcp"} {
			_, err := st.PrepareThreadWork(ctx, source, workspace, channel, nil)
			require.NoError(t, err)
		}
	}
	_, err := st.DB().ExecContext(ctx, "update sync_state set updated_at='2099-01-01T00:00:00Z'")
	require.NoError(t, err)
	status, err := st.Status(ctx)
	require.NoError(t, err)
	require.True(t, status.LastSyncAt.IsZero(), "pending work is not successful source freshness")
	_, err = st.DB().ExecContext(ctx, "insert into sync_state values ('api-bot','workspace','T1','old','2020-01-01T00:00:00Z')")
	require.NoError(t, err)
	status, err = st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "2020-01-01T00:00:00Z", status.LastSyncAt.Format(time.RFC3339))
	// A second selected root has an old skip but no pending row. Purge must
	// retire it too, without treating unrelated skips as orphan cleanup.
	noJob := batchMessage("C1", "1710000002.000000", "T1", "parent without pending work", now)
	require.NoError(t, st.UpsertMessage(ctx, noJob, nil))
	firstKey, secondKey := "T1|C1|1710000001.000000", "T1|C1|"+noJob.TS
	seedThreadWorkSkipFixtures(t, st, firstKey)
	require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", secondKey, "skip without pending work"))
	require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T2|C2|1710000001.000000", "other workspace skip"))
	beforeSkips := threadWorkSkipRows(t, st)
	opts := PurgeOptions{Before: now.Add(10 * time.Second), WorkspaceID: "T1"}
	_, err = st.PurgeMessages(ctx, opts)
	require.NoError(t, err)
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 4)
	requireThreadWorkSkips(t, st, beforeSkips)
	_, err = st.DB().ExecContext(ctx, `create trigger reject_purge before delete on messages begin select raise(abort,'synthetic_purge_failure'); end`)
	require.NoError(t, err)
	opts.Delete = true
	_, err = st.PurgeMessages(ctx, opts)
	require.ErrorContains(t, err, "synthetic_purge_failure")
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 4)
	requireThreadWorkSkips(t, st, beforeSkips)
	_, err = st.DB().ExecContext(ctx, "drop trigger reject_purge")
	require.NoError(t, err)
	_, err = st.PurgeMessages(ctx, opts)
	require.NoError(t, err)
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 2)
	requireThreadWorkSkips(t, st, beforeSkips, firstKey, secondKey)
	remaining, err := st.PendingThreadWork(ctx, "api-user", "T2", "C2")
	require.NoError(t, err)
	require.Len(t, remaining, 1)
}

func TestPhysicalDeleteRetiresThreadWork(t *testing.T) {
	for _, mode := range []string{"matched", "skip-only", "wrong-source", "wrong-workspace", "missing", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			msg := batchMessage("C1", "1710000001.000000", "T1", "parent", now)
			msg.ReplyCount = 1
			msg.Files = []MessageFile{{FileID: "F1", Name: "parent-file", RawJSON: "{}"}}
			require.NoError(t, st.UpsertMessage(ctx, msg, []Mention{{Type: "user", TargetID: "U1"}}))
			if mode != "skip-only" {
				for _, source := range []string{"api-user", "mcp"} {
					work, err := st.PrepareThreadWork(ctx, source, "T1", "C1", nil)
					require.NoError(t, err)
					require.Len(t, work, 1)
				}
			}
			key := threadWorkKey(ThreadWork{WorkspaceID: "T1", ChannelID: "C1", TS: msg.TS})
			for _, work := range []ThreadWork{
				{SourceName: "other", WorkspaceID: "T1", ChannelID: "C1", TS: msg.TS},
				{SourceName: "api-user", WorkspaceID: "T2", ChannelID: "C1", TS: msg.TS},
				{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C2", TS: msg.TS},
				{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: "1710000002.000000"},
			} {
				require.NoError(t, st.SetSyncState(ctx, work.SourceName, ThreadPendingEntityType, threadWorkKey(work), "unrelated generation"))
			}
			skipKey := "T1|C1|" + msg.TS
			beforeSkips := seedThreadWorkSkipFixtures(t, st, skipKey)
			before := map[string][]map[string]any{}
			for _, table := range []string{"messages", "message_events", "message_event_heads", "message_files", "message_mentions", "message_fts", "embedding_jobs", "sync_state"} {
				rows, err := st.QueryReadOnly(ctx, "select * from "+table)
				require.NoError(t, err)
				before[table] = rows
			}
			workspace, source, ts := "T1", msg.SourceName, msg.TS
			switch mode {
			case "wrong-source":
				source = "api-user"
			case "wrong-workspace":
				workspace = "TOTHER"
			case "missing":
				ts = "1710000099.000000"
			case "rollback":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_physical_retirement before delete on sync_state when old.source_name='api-user' and old.entity_type='thread_skip' begin select raise(abort,'synthetic_physical_retirement_failure'); end`)
				require.NoError(t, err)
			}
			removed, err := st.DeleteMessageBySource(ctx, workspace, "C1", ts, source)
			if mode == "rollback" {
				require.ErrorContains(t, err, "synthetic_physical_retirement_failure")
			} else {
				require.NoError(t, err)
			}
			wins := mode == "matched" || mode == "skip-only"
			require.Equal(t, wins, removed)
			if !wins {
				for table, rows := range before {
					after, err := st.QueryReadOnly(ctx, "select * from "+table)
					require.NoError(t, err)
					require.Equal(t, rows, after, table)
				}
				return
			}
			requireThreadWorkSkips(t, st, beforeSkips, skipKey)
			var want []map[string]any
			for _, row := range before["sync_state"] {
				ownedJob := (row["source_name"] == "api-user" || row["source_name"] == "mcp") && row["entity_type"] == ThreadPendingEntityType && row["entity_id"] == key
				ownedSkip := row["source_name"] == "api-user" && row["entity_type"] == "thread_skip" && row["entity_id"] == skipKey
				if !ownedJob && !ownedSkip {
					want = append(want, row)
				}
			}
			after, err := st.QueryReadOnly(ctx, "select * from sync_state")
			require.NoError(t, err)
			require.Equal(t, want, after)
			for table := range before {
				if table != "sync_state" {
					assertBatchCount(t, st, "select count(*) from "+table, 0)
				}
			}
			// The unrelated C1 job has a different root and remains deliberately
			// invalid. Remove only that fixture row before testing a fresh Prepare.
			require.NoError(t, st.DeleteSyncState(ctx, "api-user", ThreadPendingEntityType, threadWorkKey(ThreadWork{WorkspaceID: "T1", ChannelID: "C1", TS: "1710000002.000000"})))
			for _, source := range []string{"api-user", "mcp"} {
				work, err := st.PrepareThreadWork(ctx, source, "T1", "C1", nil)
				require.NoError(t, err)
				require.Empty(t, work, "physical deletion cannot leave an orphaned job")
			}
		})
	}
}

func seedThreadWorkSkipFixtures(t *testing.T, st *Store, key string) []map[string]any {
	t.Helper()
	for _, row := range []SyncStateRow{
		{SourceName: "api-user", EntityType: "thread_skip", EntityID: key},
		{SourceName: "api-user", EntityType: "thread_skip", EntityID: "TOTHER|C1|1710000001.000000"},
		{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T1|COTHER|1710000001.000000"},
		{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T1|C1|1710000100.000000"},
		{SourceName: "mcp", EntityType: "thread_skip", EntityID: key},
		{SourceName: "api-user", EntityType: "channel_skip", EntityID: key},
	} {
		require.NoError(t, st.SetSyncState(context.Background(), row.SourceName, row.EntityType, row.EntityID, "retained skip"))
	}
	return threadWorkSkipRows(t, st)
}

func threadWorkSkipRows(t *testing.T, st *Store) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), "select * from sync_state where entity_type in ('thread_skip','channel_skip') order by source_name,entity_type,entity_id")
	require.NoError(t, err)
	return rows
}

func requireThreadWorkSkips(t *testing.T, st *Store, before []map[string]any, removed ...string) {
	t.Helper()
	want := make([]map[string]any, 0, len(before))
	for _, row := range before {
		remove := false
		for _, key := range removed {
			remove = remove || row["source_name"] == "api-user" && row["entity_type"] == "thread_skip" && row["entity_id"] == key
		}
		if !remove {
			want = append(want, row)
		}
	}
	require.Equal(t, want, threadWorkSkipRows(t, st), "preserve all unrelated skip fields, including updated_at")
}

func TestThreadWorkGuardPrecedesAllBatchWrites(t *testing.T) {
	for _, mode := range []string{"current", "nil", "empty", "renewed", "removed", "deleted", "subtype", "parent-owner", "channel-owner", "child"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, root, nil))
			work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			require.NoError(t, err)
			require.Len(t, work, 1)
			mutations := map[string]string{
				"nil":           "delete from sync_state where entity_type='thread_pending_v1'",
				"empty":         "update sync_state set value='new-generation' where entity_type='thread_pending_v1'",
				"renewed":       "update sync_state set value='new-generation' where entity_type='thread_pending_v1'",
				"removed":       "delete from sync_state where entity_type='thread_pending_v1'",
				"deleted":       "update messages set deleted_ts='1710000002.000000'",
				"subtype":       "update messages set subtype='message_deleted'",
				"parent-owner":  "update messages set workspace_id='TOTHER'",
				"channel-owner": "update channels set workspace_id='TOTHER'",
				"child":         "update messages set thread_ts='1710000000.000000'",
			}
			if query := mutations[mode]; query != "" {
				_, err := st.DB().ExecContext(ctx, query)
				require.NoError(t, err)
			}
			current, err := st.ThreadWorkCurrent(ctx, work[0])
			require.NoError(t, err)
			allowed := mode == "current" || mode == "nil"
			require.Equal(t, mode == "current", current)
			before, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			message := batchMessage("C2", "1710000002.000000", "T2", "guarded payload", now)
			message.UserID = "U2"
			message.ReplyCount = 1
			message.Files = []MessageFile{{FileID: "F2", Name: "guarded file", RawJSON: "{}"}}
			batch := WriteBatch{
				ThreadGuard:    &work[0],
				Workspaces:     []Workspace{{ID: "T2", Name: "guarded workspace", RawJSON: "{}", UpdatedAt: now}},
				Channels:       []Channel{{ID: "C2", WorkspaceID: "T2", Name: "guarded channel", RawJSON: "{}", UpdatedAt: now}},
				Users:          []User{{ID: "U2", WorkspaceID: "T2", Name: "guarded user", RawJSON: "{}", UpdatedAt: now}},
				Messages:       []MessageWrite{{Message: message, Mentions: []Mention{{Type: "user", TargetID: "U2"}}}},
				PendingThreads: []ThreadWork{{SourceName: "api-user", WorkspaceID: "T2", ChannelID: "C2", TS: message.TS}},
				SyncStates:     []SyncStateWrite{{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T1|C1|" + root.TS, Value: "stale skip"}},
			}
			if mode == "nil" {
				batch.ThreadGuard = nil
			} else if mode == "empty" {
				batch = WriteBatch{ThreadGuard: &work[0]}
			}
			result, err := st.ApplyWriteBatch(ctx, batch)
			require.NoError(t, err)
			require.Equal(t, !allowed, result.ThreadWorkRevoked)
			for _, query := range []string{
				"select count(*) from workspaces where id='T2'",
				"select count(*) from channels where id='C2'",
				"select count(*) from users where id='U2'",
				"select count(*) from messages where channel_id='C2'",
				"select count(*) from message_files where channel_id='C2'",
				"select count(*) from message_mentions where channel_id='C2'",
				"select count(*) from message_events where channel_id='C2'",
				"select count(*) from message_event_heads where channel_id='C2'",
				"select count(*) from message_fts where message_key like 'C2|%'",
			} {
				assertBatchCount(t, st, query, map[bool]int64{true: 1, false: 0}[allowed])
			}
			if !allowed {
				after, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})
	}
}

func TestThreadCompletionKeepsNewerSkipAndRollsBack(t *testing.T) {
	for _, mode := range []string{"current", "renewed", "deleted", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, root, nil))
			work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
			require.NoError(t, err)
			require.Len(t, work, 1)
			key := "T1|C1|" + root.TS
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", key, "new attempt skip"))
			if mode == "renewed" {
				_, err = st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
				require.NoError(t, err)
			}
			if mode == "deleted" {
				_, err = st.DB().ExecContext(ctx, "update messages set subtype='message_deleted'")
				require.NoError(t, err)
			}
			if mode == "rollback" {
				_, err = st.DB().ExecContext(ctx, `create trigger reject_skip_cleanup before delete on sync_state when old.entity_type='thread_skip' begin select raise(abort,'synthetic_skip_cleanup_failure'); end`)
				require.NoError(t, err)
			}
			before, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			completed, err := st.CompleteThreadWork(ctx, work[0], key, nil)
			require.Equal(t, mode == "current", completed)
			if mode == "rollback" {
				require.ErrorContains(t, err, "synthetic_skip_cleanup_failure")
			} else {
				require.NoError(t, err)
			}
			after, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			if mode == "current" {
				require.Empty(t, after)
			} else {
				require.Equal(t, before, after)
			}
		})
	}
}
