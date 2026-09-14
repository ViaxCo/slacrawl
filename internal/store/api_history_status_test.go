package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPIHistoryStatusScopes(t *testing.T) {
	for _, source := range []string{"api-bot", "api-user"} {
		for _, since := range []string{"", "1709900000.000000"} {
			for _, mode := range []string{"missing", "empty", "complete", "pending-empty", "complete-pending"} {
				t.Run(source+"/"+since+"/"+mode, func(t *testing.T) {
					st := openBatchTestStore(t)
					ctx := context.Background()
					scope := APIHistoryScope{SourceName: source, WorkspaceID: "T1", ChannelID: "C1", Since: since}
					state := APIHistoryState{}
					if mode == "complete" || mode == "complete-pending" {
						state.Complete, state.Latest = true, "1710000000.000000"
					}
					if mode == "pending-empty" || mode == "complete-pending" {
						state.Pending = new("")
					}
					if mode != "missing" {
						seedAPIHistoryState(t, st, scope, state)
					}
					before := apiHistoryArchiveRows(t, st)
					for _, workspace := range []string{"", "T1", "T2"} {
						incomplete, err := st.HasIncompleteAPIHistory(ctx, workspace)
						require.NoError(t, err)
						require.Equal(t, workspace != "T2" && mode != "missing" && mode != "complete", incomplete)
					}
					require.Equal(t, before, apiHistoryArchiveRows(t, st), "status does not repair or fabricate checkpoints")
				})
			}
		}
	}
}

func TestAPIHistoryStatusValidatesAllOwnedKeys(t *testing.T) {
	for _, key := range []string{
		"private-key-canary", "null", `["T1","C1"]`, `["T1","C1","","extra"]`,
		`[ "T1","C1",""]`, `["T1","\u00431",""]`, `["","C1",""]`,
		`["T2","C1","NaN"]`, `["T2","C1",null]`,
	} {
		t.Run(key, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			// This sorts first: pending cannot hide a later malformed key.
			seedAPIHistoryState(t, st, APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T0", ChannelID: "C0"}, APIHistoryState{Pending: new("")})
			require.NoError(t, st.SetSyncState(ctx, "api-user", APIHistoryEntityType, key, "private-value-canary"))
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|legacy", "preserved"))
			before := apiHistoryArchiveRows(t, st)
			for _, workspace := range []string{"", "T1", "T2"} {
				incomplete, err := st.HasIncompleteAPIHistory(ctx, workspace)
				require.EqualError(t, err, "invalid API history checkpoint key")
				require.False(t, incomplete)
			}
			require.EqualError(t, st.DeleteAPIThreadSkipsIfNoPending(ctx, "T1"), "invalid API history checkpoint key")
			require.Equal(t, before, apiHistoryArchiveRows(t, st))
		})
	}
}

func TestAPIHistoryStatusValueScope(t *testing.T) {
	for _, raw := range []string{
		"private-value-canary", `{}`, `{"complete":true,"latest":""}`,
		`{"complete":false,"latest":"","pending":null}`,
		`{"complete":false,"latest":"","generation":"g"}`,
		`{"complete":false,"latest":"","pending":"","pending_latest":"1710000000.000000"}`,
		`{"complete":true,"latest":"1710000000.000000","pending":"","generation":"g","pending_latest":"1700000000.000000"}`,
		`{"complete":false,"latest":"","pending":"NaN"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			seedAPIHistoryState(t, st, APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}, APIHistoryState{Pending: new("")})
			require.NoError(t, st.SetSyncState(ctx, "api-user", APIHistoryEntityType, `["T2","C2",""]`, raw))
			before := apiHistoryArchiveRows(t, st)
			for _, workspace := range []string{"", "T2"} {
				incomplete, err := st.HasIncompleteAPIHistory(ctx, workspace)
				require.EqualError(t, err, "invalid API history checkpoint")
				require.False(t, incomplete)
			}
			incomplete, err := st.HasIncompleteAPIHistory(ctx, "T1")
			require.NoError(t, err)
			require.True(t, incomplete, "canonical foreign values are outside a workspace decision")
			require.Equal(t, before, apiHistoryArchiveRows(t, st))
		})
	}
}

func TestAPIHistorySkipCleanupSnapshot(t *testing.T) {
	for _, mode := range []string{"missing", "complete", "pending", "legacy-empty", "foreign-pending", "foreign-invalid", "unrelated", "thread-pending", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|legacy", "first"))
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|second", "second"))
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T2|legacy", "foreign"))
			scope := APIHistoryScope{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1"}
			switch mode {
			case "complete":
				seedAPIHistoryState(t, st, scope, APIHistoryState{Complete: true, Latest: "1710000000.000000"})
			case "pending":
				seedAPIHistoryState(t, st, scope, APIHistoryState{Pending: new("")})
			case "legacy-empty":
				seedAPIHistoryState(t, st, scope, APIHistoryState{})
			case "foreign-pending":
				scope.WorkspaceID = "T2"
				seedAPIHistoryState(t, st, scope, APIHistoryState{Pending: new("")})
			case "foreign-invalid":
				require.NoError(t, st.SetSyncState(ctx, "api-bot", APIHistoryEntityType, `["T2","C2",""]`, "private-invalid-canary"))
			case "unrelated":
				require.NoError(t, st.SetSyncState(ctx, "mcp", APIHistoryEntityType, "opaque", "private-invalid-canary"))
				require.NoError(t, st.SetSyncState(ctx, "api-user", "other-type", "opaque", "private-invalid-canary"))
			case "thread-pending":
				require.NoError(t, st.SetSyncState(ctx, "api-user", ThreadPendingEntityType, `["T1","C1","1"]`, "g"))
			case "rollback":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_cleanup before delete on sync_state
when old.entity_type='thread_skip' and old.entity_id='T1|second'
begin select raise(abort,'synthetic_cleanup_failure'); end`)
				require.NoError(t, err)
			}
			before := apiHistoryArchiveRows(t, st)
			err := st.DeleteAPIThreadSkipsIfNoPending(ctx, "T1")
			if mode == "rollback" {
				require.ErrorContains(t, err, "synthetic_cleanup_failure")
			} else {
				require.NoError(t, err)
			}
			if mode == "pending" || mode == "legacy-empty" || mode == "thread-pending" || mode == "rollback" {
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
			} else {
				rows, err := st.ListSyncState(ctx, "api-user", "thread_skip", 10)
				require.NoError(t, err)
				require.Equal(t, []SyncStateRow{{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T2|legacy", Value: "foreign"}}, rows)
				if mode == "foreign-invalid" {
					value, err := st.GetSyncState(ctx, "api-bot", APIHistoryEntityType, `["T2","C2",""]`)
					require.NoError(t, err)
					require.Equal(t, "private-invalid-canary", value)
				}
			}
		})
	}
}

func TestAPIHistoryCleanupSerializesWithBegin(t *testing.T) {
	for _, order := range []string{"begin-first", "cleanup-first"} {
		t.Run(order, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "history.db")
			writer, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, writer.Close()) })
			reader, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			seedBatchCatalog(t, writer, "T1", "C1", "U1", time.Unix(1710000000, 0))
			require.NoError(t, writer.SetSyncState(ctx, "api-user", "thread_skip", "T1|legacy", "preserved"))
			scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
			seedAPIHistoryState(t, writer, scope, APIHistoryState{Complete: true, Latest: "1710000000.000000"})
			if order == "cleanup-first" {
				require.NoError(t, reader.DeleteAPIThreadSkipsIfNoPending(ctx, "T1"))
				_, err := writer.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000100.000000")
				require.NoError(t, err)
				rows, err := reader.ListSyncState(ctx, "api-user", "thread_skip", 10)
				require.NoError(t, err)
				require.Empty(t, rows, "later work does not undo a previously valid cleanup")
			} else {
				q, commit, rollback, err := writer.beginMessageTransaction(ctx, true)
				require.NoError(t, err)
				defer rollback()
				started, done := make(chan struct{}), make(chan error, 1)
				go func() {
					close(started)
					done <- reader.DeleteAPIThreadSkipsIfNoPending(ctx, "T1")
				}()
				<-started
				raw, err := json.Marshal(APIHistoryState{Complete: true, Latest: "1710000000.000000", Pending: new(""), Generation: "writer-generation", PendingLatest: "1710000100.000000"})
				require.NoError(t, err)
				_, err = q.ExecContext(ctx, "update sync_state set value=? where entity_type=?", string(raw), APIHistoryEntityType)
				require.NoError(t, err)
				require.NoError(t, commit())
				require.NoError(t, <-done)
				rows, err := reader.ListSyncState(ctx, "api-user", "thread_skip", 10)
				require.NoError(t, err)
				require.Len(t, rows, 1, "cleanup must read history after acquiring its writer snapshot")
			}
			incomplete, err := reader.HasIncompleteAPIHistory(ctx, "T1")
			require.NoError(t, err)
			require.True(t, incomplete)
		})
	}
}

func TestAPIReplyCollisionQueuesRequestedRoot(t *testing.T) {
	for _, mode := range []string{"create", "hint-free-root", "renew", "discovery-existing", "no-collision", "no-option", "guarded", "missing", "foreign", "deleted", "child", "tombstone", "rollback", "history-revoked", "thread-revoked"} {
		t.Run(mode, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			now := time.Unix(1710000000, 0)
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			root := batchMessage("C1", "1710000001.000000", "T1", "owned root", now)
			root.ReplyCount = 1
			if mode == "hint-free-root" {
				root.ReplyCount = 0
			}
			if mode == "foreign" {
				root.WorkspaceID = "T2"
			}
			if mode == "deleted" {
				root.DeletedTS = "1710000010.000000"
			}
			if mode == "child" {
				root.ThreadTS = "1710000000.000000"
			}
			if mode != "missing" {
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
			}
			foreign := batchMessage("C1", "1710000002.000000", "T2", "foreign collision canary", now)
			require.NoError(t, st.UpsertMessage(ctx, foreign, nil))
			request := ThreadWork{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}
			var old ThreadWork
			if mode == "renew" || mode == "discovery-existing" || mode == "guarded" || mode == "thread-revoked" {
				work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
				require.NoError(t, err)
				require.Len(t, work, 1)
				old = work[0]
			}
			incoming := foreign
			incoming.WorkspaceID, incoming.Text = "T1", "rejected replacement"
			good := batchMessage("C1", "1710000003.000000", "T1", "admitted sibling", now)
			batch := WriteBatch{Messages: []MessageWrite{{Message: good}, {Message: incoming, SkipWorkspaceCollision: true}}, PendingThreadOnCollision: &request}
			if mode == "no-collision" {
				batch.Messages = batch.Messages[:1]
			}
			if mode == "no-option" || mode == "guarded" {
				batch.PendingThreadOnCollision = nil
			}
			if mode == "guarded" || mode == "thread-revoked" {
				batch.ThreadGuard = &old
			}
			if mode == "discovery-existing" {
				batch.PendingThreads = []ThreadWork{request}
				batch.ThreadDiscovery = &ThreadWorkDiscovery{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", ExcludedTS: map[string]struct{}{root.TS: {}}}
			}
			if mode == "tombstone" {
				root.DeletedTS = "1710000010.000000"
				batch.Messages = append(batch.Messages, MessageWrite{Message: root})
			}
			if mode == "rollback" {
				_, err := st.DB().ExecContext(ctx, "create trigger reject_collision_queue before insert on sync_state when new.entity_type='thread_pending_v1' begin select raise(abort,'collision queue rollback'); end")
				require.NoError(t, err)
			}
			if mode == "history-revoked" {
				scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
				attempt, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000100.000000")
				require.NoError(t, err)
				_, err = st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000200.000000")
				require.NoError(t, err)
				batch.HistoryGuard = &attempt
			}
			if mode == "thread-revoked" {
				_, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", nil)
				require.NoError(t, err)
			}
			foreignBefore, err := st.QueryReadOnly(ctx, "select * from messages where workspace_id='T2' order by ts")
			require.NoError(t, err)
			before := apiHistoryArchiveRows(t, st)
			result, err := st.ApplyWriteBatch(ctx, batch)
			switch mode {
			case "rollback":
				require.ErrorContains(t, err, "collision queue rollback")
				require.Equal(t, WriteBatchResult{}, result)
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
				return
			case "history-revoked":
				require.ErrorIs(t, err, ErrAPIHistorySuperseded)
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
				return
			case "thread-revoked":
				require.NoError(t, err)
				require.True(t, result.ThreadWorkRevoked)
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
				return
			}
			require.NoError(t, err)
			require.Len(t, result.CollisionsSkipped, map[bool]int{true: 0, false: 1}[mode == "no-collision"])
			foreignAfter, err := st.QueryReadOnly(ctx, "select * from messages where workspace_id='T2' order by ts")
			require.NoError(t, err)
			require.Equal(t, foreignBefore, foreignAfter)
			pending, err := st.PendingThreadWork(ctx, "api-user", "T1", "C1")
			require.NoError(t, err)
			switch mode {
			case "create", "hint-free-root", "renew", "discovery-existing":
				require.Len(t, result.PendingThreads, 1)
				require.Equal(t, result.PendingThreads, pending)
				require.Equal(t, request.TS, pending[0].TS, "queue requested root, never collided child")
				require.NotEmpty(t, pending[0].Generation)
				require.NotEqual(t, old.Generation, pending[0].Generation)
			case "guarded":
				require.Empty(t, result.PendingThreads)
				require.Equal(t, []ThreadWork{old}, pending, "an owned reply retains its current generation")
			default:
				require.Empty(t, result.PendingThreads)
				require.Empty(t, pending)
			}
			requireMessageFTSParity(t, st)
		})
	}
}
