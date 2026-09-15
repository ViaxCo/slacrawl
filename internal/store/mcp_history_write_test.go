package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMCPHistoryGuardsAllWrites(t *testing.T) {
	for _, state := range []string{"pending", "complete", "new-pending", "new-complete", "missing", "replaced", "malformed", "foreign", "empty-revision"} {
		for _, operation := range []string{"batch", "empty", "discovery", "prepare", "tombstone"} {
			t.Run(state+"/"+operation, func(t *testing.T) {
				ctx := context.Background()
				st, other := mcpHistoryWriteStores(t)
				now := time.Unix(1710000000, 0).UTC()
				seedBatchCatalog(t, st, "T1", "C1", "U1", now)
				root := batchMessage("C1", "1710000001.000000", "T1", "original", now)
				root.SourceName, root.SourceRank, root.ReplyCount = "mcp", 4, 1
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				queued, err := st.ApplyWriteBatch(ctx, WriteBatch{PendingThreads: []ThreadWork{{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}}})
				require.NoError(t, err)
				require.Len(t, queued.PendingThreads, 1)
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|"+root.TS, "retained skip"))
				scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
				work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
				key, err := scope.key()
				require.NoError(t, err)
				switch state {
				case "complete":
					done, err := st.CompleteMCPHistory(ctx, work, root.TS)
					require.NoError(t, err)
					require.True(t, done)
				case "new-pending", "new-complete":
					next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
					if state == "new-complete" {
						done, err := other.CompleteMCPHistory(ctx, next, root.TS)
						require.NoError(t, err)
						require.True(t, done)
					}
				case "missing":
					require.NoError(t, other.DeleteSyncState(ctx, "mcp", MCPHistoryEntityType, key))
				case "replaced":
					require.NoError(t, other.SetSyncState(ctx, "mcp", MCPHistoryEntityType, key, `{"complete":true,"latest":"","pending":null,"revision":"replacement"}`))
				case "malformed":
					require.NoError(t, other.SetSyncState(ctx, "mcp", MCPHistoryEntityType, key, "private-checkpoint-canary"))
				case "foreign":
					require.NoError(t, other.UpsertWorkspace(ctx, Workspace{ID: "T2", Name: "foreign", RawJSON: "{}", UpdatedAt: now}))
					_, err := other.DB().ExecContext(ctx, "update channels set workspace_id='T2' where id='C1'")
					require.NoError(t, err)
				case "empty-revision":
					work.Revision = ""
				}
				before := apiHistoryArchiveRows(t, st)
				current, checkErr := st.MCPHistoryCurrent(ctx, work)
				accepted := state == "pending" || state == "complete"
				require.Equal(t, accepted, current)
				if state == "malformed" || state == "foreign" || state == "empty-revision" {
					require.Error(t, checkErr)
				} else {
					require.NoError(t, checkErr)
				}
				batch := WriteBatch{MCPHistoryGuard: &work}
				changed := root
				changed.Text, changed.NormalizedText, changed.RawJSON = "new <@U2>", "new U2", `{"text":"new"}`
				switch operation {
				case "batch":
					batch.Workspaces = []Workspace{{ID: "TNEW", Name: "new", RawJSON: "{}", UpdatedAt: now}}
					batch.Channels = []Channel{{ID: "CNEW", WorkspaceID: "T1", Name: "new", RawJSON: "{}", UpdatedAt: now}}
					batch.Users = []User{{ID: "UNEW", WorkspaceID: "T1", Name: "new", RawJSON: "{}", UpdatedAt: now}}
					batch.Messages = []MessageWrite{{Message: changed, Mentions: []Mention{{Type: "user", TargetID: "U2"}}}}
					batch.SyncStates = []SyncStateWrite{{SourceName: "fixture", EntityType: "guard", EntityID: "new", Value: "new"}}
					batch.SyncStateDeletes = []SyncStateDelete{{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T1|C1|" + root.TS}}
				case "discovery":
					batch.PendingThreads = []ThreadWork{{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}}
					batch.ThreadDiscovery = &ThreadWorkDiscovery{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1"}
				case "tombstone":
					changed.DeletedTS = "1710000009.000000"
					batch.Messages = []MessageWrite{{Message: changed}}
				}
				var result WriteBatchResult
				var prepared []ThreadWork
				if operation == "prepare" {
					prepared, err = st.PrepareMCPThreadWork(ctx, work)
				} else {
					result, err = st.ApplyWriteBatch(ctx, batch)
				}
				if !accepted {
					require.Error(t, err)
					if state == "malformed" {
						require.EqualError(t, err, "invalid local MCP history checkpoint")
					} else if state == "foreign" {
						require.True(t, IsWorkspaceCollision(err, "channel"))
					} else if state == "empty-revision" {
						require.EqualError(t, err, "MCP history work requires an attempt revision")
					} else {
						require.ErrorIs(t, err, ErrMCPHistorySuperseded)
					}
					require.NotContains(t, err.Error(), "private-checkpoint-canary")
					require.Equal(t, WriteBatchResult{}, result)
					require.Nil(t, prepared)
					require.Equal(t, before, apiHistoryArchiveRows(t, st), "no metadata, derived rows, retirement or generation renewal")
					return
				}
				require.NoError(t, err)
				switch operation {
				case "batch":
					require.Equal(t, 1, result.MessagesWritten)
					assertBatchFTSContent(t, st, "C1|"+root.TS, changed.NormalizedText)
					requireMessageFTSParity(t, st)
				case "prepare":
					require.Len(t, prepared, 1)
					require.NotEqual(t, queued.PendingThreads[0].Generation, prepared[0].Generation)
				case "tombstone":
					pending, err := st.PendingThreadWork(ctx, "mcp", "T1", "C1")
					require.NoError(t, err)
					require.Empty(t, pending)
					assertBatchCount(t, st, "select count(*) from sync_state where entity_type='thread_skip'", 0)
				case "empty", "discovery":
					require.Equal(t, before, apiHistoryArchiveRows(t, st))
				}
			})
		}
	}
}

func TestMCPHistoryGuardAllowsFirstIntake(t *testing.T) {
	ctx := context.Background()
	st := openBatchTestStore(t)
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.EnsureWorkspace(ctx, Workspace{ID: "T1", Name: "first", RawJSON: "{}", UpdatedAt: now}))
	work := beginHistoryTest(t, st, MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}, MCPHistoryOptions{})
	current, err := st.MCPHistoryCurrent(ctx, work)
	require.NoError(t, err)
	require.True(t, current, "the first channel row is intentionally not present yet")
	_, err = st.ApplyWriteBatch(ctx, WriteBatch{MCPHistoryGuard: &work, Channels: []Channel{{ID: "C1", WorkspaceID: "T1", Name: "first", RawJSON: "{}", UpdatedAt: now}}})
	require.NoError(t, err)
	done, err := st.CompleteMCPHistory(ctx, work, "")
	require.NoError(t, err)
	require.True(t, done)
	current, err = st.MCPHistoryCurrent(ctx, work)
	require.NoError(t, err)
	require.True(t, current)
}

func TestMCPHistoryFencesRemainingBatches(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprint(completed), func(t *testing.T) {
			ctx := context.Background()
			st, other := mcpHistoryWriteStores(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
			work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
			batch := WriteBatch{MCPHistoryGuard: &work, ThreadDiscovery: &ThreadWorkDiscovery{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1"}}
			for i := 0; i < 500; i++ {
				message := batchMessage("C1", fmt.Sprintf("171000%04d.000000", i), "T1", "committed", now)
				message.SourceName, message.SourceRank = "mcp", 4
				if i == 0 {
					message.ReplyCount = 1
				}
				batch.Messages = append(batch.Messages, MessageWrite{Message: message})
			}
			written, err := st.ApplyWriteBatch(ctx, batch)
			require.NoError(t, err)
			require.Equal(t, 500, written.MessagesWritten)
			require.Len(t, written.PendingThreads, 1)
			next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
			if completed {
				done, err := other.CompleteMCPHistory(ctx, next, "1710000499.000000")
				require.NoError(t, err)
				require.True(t, done)
			}
			before := apiHistoryArchiveRows(t, st)
			batch.Messages = []MessageWrite{{Message: batchMessage("C1", "1710000500.000000", "T1", "stale-canary", now)}}
			result, err := st.ApplyWriteBatch(ctx, batch)
			require.ErrorIs(t, err, ErrMCPHistorySuperseded)
			require.Equal(t, WriteBatchResult{}, result)
			require.Equal(t, before, apiHistoryArchiveRows(t, st))
			assertBatchCount(t, st, "select count(*) from messages", 500)
		})
	}
}

func TestMCPHistoryCompletedDiscovery(t *testing.T) {
	for _, state := range []string{"own", "new-pending", "new-complete"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			st, other := mcpHistoryWriteStores(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "codex"}
			work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
			done, err := st.CompleteMCPHistory(ctx, work, "")
			require.NoError(t, err)
			require.True(t, done)
			root := batchMessage("C1", "1710000001.000000", "T1", "retained root", now)
			root.ReplyCount = 1
			require.NoError(t, other.UpsertMessage(ctx, root, nil))
			if state != "own" {
				next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
				if state == "new-complete" {
					done, err := other.CompleteMCPHistory(ctx, next, root.TS)
					require.NoError(t, err)
					require.True(t, done)
				}
			}
			roots, err := st.ChannelThreadRoots(ctx, "T1", "C1")
			require.NoError(t, err)
			require.Len(t, roots, 1)
			before := apiHistoryArchiveRows(t, st)
			result, err := st.ApplyWriteBatch(ctx, WriteBatch{MCPHistoryGuard: &work,
				ThreadDiscovery: &ThreadWorkDiscovery{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1"},
				PendingThreads:  []ThreadWork{{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: roots[0].TS}},
			})
			if state == "own" {
				require.NoError(t, err)
				require.Len(t, result.PendingThreads, 1)
				require.Equal(t, root.TS, result.PendingThreads[0].TS)
				require.True(t, readHistoryTest(t, st, scope).Complete)
			} else {
				require.ErrorIs(t, err, ErrMCPHistorySuperseded)
				require.Equal(t, WriteBatchResult{}, result)
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
			}
		})
	}
}

// No-tool reconciliation owns current tombstones and live jobs independently;
// this composes the Store order rather than claiming two concurrent full Syncs.
func TestMCPHistoryNoToolReconciliationRemainsIndependent(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprint(deleted), func(t *testing.T) {
			ctx := context.Background()
			st, other := mcpHistoryWriteStores(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			for _, ts := range []string{"1710000001.000000", "1710000002.000000"} {
				root := batchMessage("C1", ts, "T1", "root", now)
				root.ReplyCount = 1
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|"+ts, "retained"))
			}
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
			work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
			done, err := st.CompleteMCPHistory(ctx, work, "")
			require.NoError(t, err)
			require.True(t, done)
			next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
			owned, err := other.PrepareMCPThreadWork(ctx, next)
			require.NoError(t, err)
			require.Len(t, owned, 2)
			if deleted {
				_, err := other.DB().ExecContext(ctx, "update messages set subtype='message_deleted' where channel_id='C1' and ts='1710000001.000000'")
				require.NoError(t, err)
			}
			before, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='mcp' and entity_type='history_work_v1'")
			require.NoError(t, err)
			remaining, err := st.ReconcileThreadWork(ctx, "mcp", "T1", "C1")
			require.NoError(t, err)
			expected := owned
			if deleted {
				expected = owned[1:]
			}
			require.Equal(t, expected, remaining, "newer live generations are neither adopted nor renewed")
			after, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='mcp' and entity_type='history_work_v1'")
			require.NoError(t, err)
			require.Equal(t, before, after)
			skips, err := st.QueryReadOnly(ctx, "select entity_id from sync_state where entity_type='thread_skip' order by entity_id")
			require.NoError(t, err)
			expectedSkips := make([]map[string]any, 0, len(expected))
			for _, item := range expected {
				expectedSkips = append(expectedSkips, map[string]any{"entity_id": "T1|C1|" + item.TS})
			}
			require.Equal(t, expectedSkips, skips)
		})
	}
}

func TestMCPHistoryGuardPreservesPriorCommitsOnFailure(t *testing.T) {
	for _, mode := range []string{"canceled", "batch-rollback", "prepare-rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			work := beginHistoryTest(t, st, MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}, MCPHistoryOptions{})
			root := batchMessage("C1", "1710000001.000000", "T1", "committed", now)
			root.ReplyCount = 1
			_, err := st.ApplyWriteBatch(ctx, WriteBatch{MCPHistoryGuard: &work, Messages: []MessageWrite{{Message: root}},
				PendingThreads: []ThreadWork{{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: root.TS}},
			})
			require.NoError(t, err)
			switch mode {
			case "batch-rollback":
				_, err = st.DB().ExecContext(ctx, "create trigger fail_second before insert on messages when new.ts='1710000002.000000' begin select raise(abort,'synthetic batch failure'); end")
			case "prepare-rollback":
				_, err = st.DB().ExecContext(ctx, "create trigger fail_renewal before update on sync_state when new.entity_type='thread_pending_v1' begin select raise(abort,'synthetic preparation failure'); end")
			}
			require.NoError(t, err)
			before := apiHistoryArchiveRows(t, st)
			attemptCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			if mode == "prepare-rollback" {
				_, err = st.PrepareMCPThreadWork(attemptCtx, work)
			} else {
				changed := root
				changed.Text = "must roll back"
				next := root
				next.TS = "1710000002.000000"
				_, err = st.ApplyWriteBatch(attemptCtx, WriteBatch{MCPHistoryGuard: &work, Messages: []MessageWrite{{Message: changed}, {Message: next}}})
			}
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorContains(t, err, "synthetic")
			}
			require.Equal(t, before, apiHistoryArchiveRows(t, st))
			state := readHistoryTest(t, st, work.MCPHistoryScope)
			require.Equal(t, work.Revision, state.Revision)
			require.Equal(t, new(""), state.Pending)
			require.False(t, state.Complete)
		})
	}
}

func mcpHistoryWriteStores(t *testing.T) (*Store, *Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history-writes.db")
	first, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	return first, second
}
