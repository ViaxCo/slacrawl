package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReturnedThreadRootsUseFinalScopedEvidence(t *testing.T) {
	for _, mode := range []string{"root-child", "child-root", "retained-child", "child-only", "empty", "priority-child", "foreign", "deleted_ts", "subtype", "positive-duplicate"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.SourceName, root.SourceRank = "mcp", 4
			child := root
			child.TS, child.ThreadTS, child.Text = "1710000002.000000", root.TS, "child"
			writes := []MessageWrite{{Message: root}, {Message: child}}
			returned := []string{root.TS, child.TS}
			var hints []string
			switch mode {
			case "child-root":
				writes[0], writes[1] = writes[1], writes[0]
			case "retained-child":
				require.NoError(t, st.UpsertMessage(ctx, child, nil))
				writes, returned = writes[:1], returned[:1]
			case "child-only":
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				writes, returned = writes[1:], returned[1:]
			case "empty":
				returned = nil
			case "priority-child":
				retained := child
				retained.SourceName, retained.SourceRank, retained.ThreadTS = "api-user", 1, ""
				require.NoError(t, st.UpsertMessage(ctx, retained, nil))
				writes[1].PreserveHigherPriority = true
			case "positive-duplicate":
				hinted := root
				hinted.ReplyCount = 1
				writes = []MessageWrite{{Message: hinted}, {Message: root}}
				hints, returned = []string{root.TS}, []string{root.TS, root.TS}
			}
			_, err := st.ApplyWriteBatch(ctx, WriteBatch{Messages: writes})
			require.NoError(t, err)
			switch mode {
			case "foreign":
				_, err = st.DB().ExecContext(ctx, "update messages set workspace_id='T2' where ts=?", root.TS)
			case "deleted_ts":
				_, err = st.DB().ExecContext(ctx, "update messages set deleted_ts='1710000009.000000' where ts=?", root.TS)
			case "subtype":
				_, err = st.DB().ExecContext(ctx, "update messages set subtype='message_deleted' where ts=?", root.TS)
			}
			require.NoError(t, err)
			require.NoError(t, st.SetSyncState(ctx, "mcp", ThreadPendingEntityType, threadWorkKey(ThreadWork{WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}), "untouched-generation"))
			before, err := st.QueryReadOnly(ctx, "select * from sync_state")
			require.NoError(t, err)
			roots, err := st.ReturnedThreadRoots(ctx, "T1", "C1", returned, hints)
			require.NoError(t, err)
			want := mode == "root-child" || mode == "child-root" || mode == "retained-child" || mode == "positive-duplicate"
			if want {
				require.Equal(t, []ThreadRoot{{ChannelID: "C1", TS: root.TS}}, roots)
			} else {
				require.Empty(t, roots)
			}
			after, err := st.QueryReadOnly(ctx, "select * from sync_state")
			require.NoError(t, err)
			require.Equal(t, before, after, "scoped root selection must never readjust ordinary work")
			other, err := st.ReturnedThreadRoots(ctx, "T2", "C1", returned, hints)
			require.NoError(t, err)
			require.Empty(t, other, "channel ownership also bounds retained message ownership")
		})
	}
}

func TestThreadWorkReconciliationDoesNotRenewOrDiscover(t *testing.T) {
	for _, mode := range []string{"live", "fresh", "deleted_ts", "subtype", "missing", "foreign", "child", "rollback", "scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "root", now)
			root.ReplyCount = 1
			require.NoError(t, st.UpsertMessage(ctx, root, nil))
			if mode != "fresh" {
				_, err := st.PrepareThreadWork(ctx, "mcp", "T1", "C1", nil)
				require.NoError(t, err)
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|"+root.TS, "existing skip"))
			}
			unqueued := root
			unqueued.TS = "1710000003.000000"
			require.NoError(t, st.UpsertMessage(ctx, unqueued, nil))
			switch mode {
			case "deleted_ts", "rollback":
				_, err := st.DB().ExecContext(ctx, "update messages set deleted_ts='1710000009.000000',reply_count=0 where ts=?", root.TS)
				require.NoError(t, err)
			case "subtype":
				_, err := st.DB().ExecContext(ctx, "update messages set subtype='message_deleted',reply_count=0 where ts=?", root.TS)
				require.NoError(t, err)
			case "missing":
				_, err := st.DB().ExecContext(ctx, "delete from messages where ts=?", root.TS)
				require.NoError(t, err)
			case "foreign":
				_, err := st.DB().ExecContext(ctx, "update messages set workspace_id='T2' where ts=?", root.TS)
				require.NoError(t, err)
			case "child":
				_, err := st.DB().ExecContext(ctx, "update messages set thread_ts=? where ts=?", unqueued.TS, root.TS)
				require.NoError(t, err)
			case "scope":
				seedBatchCatalog(t, st, "T2", "C2", "U2", now)
				foreign := root
				foreign.WorkspaceID, foreign.ChannelID = "T2", "C2"
				require.NoError(t, st.UpsertMessage(ctx, foreign, nil))
				_, err := st.PrepareThreadWork(ctx, "mcp", "T2", "C2", nil)
				require.NoError(t, err)
				_, err = st.DB().ExecContext(ctx, "update messages set subtype='message_deleted' where channel_id='C2'")
				require.NoError(t, err)
			}
			if mode == "rollback" {
				_, err := st.DB().ExecContext(ctx, "create trigger reject_reconciled_skip before delete on sync_state when old.entity_type='thread_skip' begin select raise(abort,'synthetic_reconciliation_failure'); end")
				require.NoError(t, err)
			}
			before, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			messages, err := st.QueryReadOnly(ctx, "select * from messages order by channel_id,ts")
			require.NoError(t, err)
			work, err := st.ReconcileThreadWork(ctx, "mcp", "T1", "C1")
			failed := mode == "missing" || mode == "foreign" || mode == "child" || mode == "rollback"
			if failed {
				if mode == "rollback" {
					require.ErrorContains(t, err, "synthetic_reconciliation_failure")
				} else {
					require.ErrorContains(t, err, "pending thread parent is missing or inconsistent")
				}
			} else {
				require.NoError(t, err)
				if mode == "live" || mode == "scope" {
					require.Len(t, work, 1)
					require.Equal(t, root.TS, work[0].TS)
				} else {
					require.Empty(t, work)
				}
			}
			after, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			if mode == "deleted_ts" || mode == "subtype" {
				require.Empty(t, after)
			} else {
				require.Equal(t, before, after, "cleanup neither renews live jobs nor discovers hints, and errors roll back all retirement")
			}
			afterMessages, err := st.QueryReadOnly(ctx, "select * from messages order by channel_id,ts")
			require.NoError(t, err)
			require.Equal(t, messages, afterMessages)
		})
	}
}
