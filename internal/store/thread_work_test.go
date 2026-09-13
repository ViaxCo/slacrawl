package store

import (
	"context"
	"fmt"
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
	first, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
	require.NoError(t, err)
	require.Len(t, first, 61, "the authoritative backlog has no reporting limit")
	_, err = st.DB().ExecContext(ctx, "update messages set reply_count=0 where channel_id='C1'")
	require.NoError(t, err)
	renewed, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
	require.NoError(t, err)
	require.Len(t, renewed, 61, "pending work survives overwritten hints")
	for i := range first {
		require.Equal(t, first[i].TS, renewed[i].TS)
		require.NotEqual(t, first[i].Generation, renewed[i].Generation)
		require.NoError(t, st.CompleteThreadWork(ctx, first[i], ""))
	}
	remaining, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
	require.NoError(t, err)
	require.Equal(t, renewed, remaining, "stale completion cannot delete a newer generation")
	other, err := st.PrepareThreadWork(ctx, "api-user", "T2", "C2")
	require.NoError(t, err)
	require.Empty(t, other)
	for _, item := range renewed {
		require.NoError(t, st.CompleteThreadWork(ctx, item, ""))
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
	for _, marker := range []string{"deleted_ts", "subtype"} {
		t.Run(marker, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			msg := batchMessage("C1", "1710000001.000000", "T1", "parent", now)
			msg.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, msg, nil))
			for _, source := range []string{"api-user", "mcp"} {
				_, err := st.PrepareThreadWork(ctx, source, "T1", "C1")
				require.NoError(t, err)
			}
			// A share merge can write tombstones directly. Preparation must
			// reconcile those stored markers before attempting replies.
			query := "update messages set deleted_ts='1710000002.000000'"
			if marker == "subtype" {
				query = "update messages set subtype='message_deleted'"
			}
			_, err := st.DB().ExecContext(ctx, query)
			require.NoError(t, err)
			queued, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
			require.NoError(t, err)
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
		work, err := st.PrepareThreadWork(ctx, "api-user", parts[0], parts[1])
		require.NoError(t, err)
		require.Len(t, work, 1)
		keys = append(keys, threadWorkKey(work[0]))
		pending, err := st.PendingThreadWork(ctx, "api-user", parts[0], parts[1])
		require.NoError(t, err)
		require.Equal(t, work, pending)
	}
	require.NotEqual(t, keys[0], keys[1])
	_, err := st.PrepareThreadWork(ctx, "api-user", "T", "C")
	require.True(t, IsWorkspaceCollision(err, "channel"))
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
			if mode == "canceled" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			_, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorContains(t, err, "pending thread parent is missing or inconsistent")
			}
			value, err := st.GetSyncState(context.Background(), work.SourceName, ThreadPendingEntityType, threadWorkKey(work))
			require.NoError(t, err)
			require.Equal(t, "unchanged", value)
		})
	}
}

func TestThreadWorkDeletionLifecycle(t *testing.T) {
	for _, mode := range []string{"batch", "standalone", "losing", "resurrected", "collision", "rollback", "retention-skipped", "subtype-batch", "subtype-standalone"} {
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
			case "retention-skipped":
				opts := PurgeOptions{Before: now.Add(10 * time.Second), Delete: true, WorkspaceID: "T1"}
				_, err := st.PurgeMessages(ctx, opts)
				require.NoError(t, err)
				for _, source := range []string{"api-user", "mcp"} {
					require.NoError(t, st.SetSyncState(ctx, source, ThreadPendingEntityType, key, "pending"))
				}
				batch.Messages[0].EnforceRetention = true
			}
			var err error
			if mode == "standalone" {
				err = st.MarkMessageDeleted(ctx, deletion, nil)
			} else if mode == "subtype-standalone" {
				err = st.UpsertMessage(ctx, deletion, nil)
			} else {
				_, err = st.ApplyWriteBatch(ctx, batch)
			}
			if mode == "rollback" {
				require.ErrorContains(t, err, "synthetic_retire_failure")
			} else {
				require.NoError(t, err)
			}
			wins := mode == "batch" || mode == "standalone" || mode == "subtype-batch" || mode == "subtype-standalone"
			assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1' and source_name in ('api-user','mcp')", map[bool]int64{true: 0, false: 2}[wins])
			assertBatchCount(t, st, "select count(*) from sync_state where source_name='other'", 1)
			if mode != "retention-skipped" {
				assertBatchCount(t, st, "select count(*) from messages where trim(coalesce(deleted_ts,''))<>'' or subtype='message_deleted'", map[bool]int64{true: 1, false: 0}[wins])
				assertBatchCount(t, st, "select count(*) from message_files where deleted_at is null", map[bool]int64{true: 0, false: 1}[mode == "standalone"])
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
			_, err := st.PrepareThreadWork(ctx, source, workspace, channel)
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
	opts := PurgeOptions{Before: now.Add(10 * time.Second), WorkspaceID: "T1"}
	_, err = st.PurgeMessages(ctx, opts)
	require.NoError(t, err)
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 4)
	_, err = st.DB().ExecContext(ctx, `create trigger reject_purge before delete on messages begin select raise(abort,'synthetic_purge_failure'); end`)
	require.NoError(t, err)
	opts.Delete = true
	_, err = st.PurgeMessages(ctx, opts)
	require.ErrorContains(t, err, "synthetic_purge_failure")
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 4)
	_, err = st.DB().ExecContext(ctx, "drop trigger reject_purge")
	require.NoError(t, err)
	_, err = st.PurgeMessages(ctx, opts)
	require.NoError(t, err)
	assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_pending_v1'", 2)
	remaining, err := st.PendingThreadWork(ctx, "api-user", "T2", "C2")
	require.NoError(t, err)
	require.Len(t, remaining, 1)
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
			work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
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
			work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
			require.NoError(t, err)
			require.Len(t, work, 1)
			key := "T1|C1|" + root.TS
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", key, "new attempt skip"))
			if mode == "renewed" {
				_, err = st.PrepareThreadWork(ctx, "api-user", "T1", "C1")
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
			err = st.CompleteThreadWork(ctx, work[0], key)
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
