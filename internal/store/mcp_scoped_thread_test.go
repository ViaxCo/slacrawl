package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMCPReturnedThreadWorkAcquiresOnlySelectedRoots(t *testing.T) {
	for _, mode := range []string{"pending", "complete", "new-pending", "new-complete", "missing", "malformed", "foreign", "canceled", "rollback", "empty"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st, other := mcpHistoryWriteStores(t)
			now := time.Unix(1710000000, 0).UTC()
			seedBatchCatalog(t, st, "T1", "C1", "U1", now)
			var roots []Message
			for i := 1; i <= 3; i++ {
				root := batchMessage("C1", fmt.Sprintf("171000000%d.000000", i), "T1", "root", now)
				root.SourceName, root.SourceRank = "mcp", 4
				if i == 1 {
					root.ReplyCount = 1
				}
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				roots = append(roots, root)
			}
			child := roots[1]
			child.TS, child.ThreadTS = "1710000010.000000", roots[1].TS
			require.NoError(t, st.UpsertMessage(ctx, child, nil))
			queued, err := st.ApplyWriteBatch(ctx, WriteBatch{PendingThreads: []ThreadWork{
				{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: roots[0].TS},
				{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1", TS: roots[2].TS},
				{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: roots[0].TS},
			}})
			require.NoError(t, err)
			require.Len(t, queued.PendingThreads, 3)
			require.NoError(t, st.SetSyncState(ctx, "mcp", ThreadPendingEntityType, "unselected-malformed-key", "untouched"))
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|"+roots[0].TS, "unchanged"))
			seedBatchCatalog(t, st, "T2", "C2", "U2", now)
			foreign := roots[0]
			foreign.WorkspaceID, foreign.ChannelID = "T2", "C2"
			require.NoError(t, st.UpsertMessage(ctx, foreign, nil))
			_, err = st.ApplyWriteBatch(ctx, WriteBatch{PendingThreads: []ThreadWork{{SourceName: "mcp", WorkspaceID: "T2", ChannelID: "C2", TS: foreign.TS}}})
			require.NoError(t, err)
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference", Since: "100"}
			history := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
			switch mode {
			case "complete":
				done, err := st.CompleteMCPHistory(ctx, history, "")
				require.NoError(t, err)
				require.True(t, done)
			case "new-pending", "new-complete":
				next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
				if mode == "new-complete" {
					done, err := other.CompleteMCPHistory(ctx, next, "")
					require.NoError(t, err)
					require.True(t, done)
				}
			case "missing", "malformed":
				key, err := scope.key()
				require.NoError(t, err)
				if mode == "missing" {
					require.NoError(t, other.DeleteSyncState(ctx, "mcp", MCPHistoryEntityType, key))
				} else {
					require.NoError(t, other.SetSyncState(ctx, "mcp", MCPHistoryEntityType, key, "private-checkpoint-canary"))
				}
			case "foreign":
				_, err := other.DB().ExecContext(ctx, "update channels set workspace_id='T2' where id='C1'")
				require.NoError(t, err)
			case "rollback":
				_, err := st.DB().ExecContext(ctx, "create trigger reject_second_scoped_job before insert on sync_state when new.entity_type='thread_pending_v1' and new.entity_id='[\"T1\",\"C1\",\"1710000002.000000\"]' begin select raise(abort,'synthetic acquisition failure'); end")
				require.NoError(t, err)
			}
			before := apiHistoryArchiveRows(t, st)
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			returned := []string{roots[0].TS, roots[0].TS, roots[1].TS, roots[2].TS, child.TS, "1710000999.000000"}
			hints := []string{child.TS, "1710000999.000000"}
			if mode == "empty" {
				returned = nil
			}
			work, err := st.PrepareMCPReturnedThreadWork(callCtx, history, returned, hints)
			switch mode {
			case "new-pending", "new-complete", "missing":
				require.ErrorIs(t, err, ErrMCPHistorySuperseded)
			case "malformed":
				require.EqualError(t, err, "invalid local MCP history checkpoint")
				require.NotContains(t, err.Error(), "private-checkpoint-canary")
			case "foreign":
				require.True(t, IsWorkspaceCollision(err, "channel"))
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "rollback":
				require.ErrorContains(t, err, "synthetic acquisition failure")
			default:
				require.NoError(t, err)
			}
			if err != nil || mode == "empty" {
				require.Empty(t, work)
				require.Equal(t, before, apiHistoryArchiveRows(t, st), "including rollback of the first selected renewal")
				return
			}
			require.Len(t, work, 2)
			require.Equal(t, []string{roots[0].TS, roots[1].TS}, []string{work[0].TS, work[1].TS})
			require.NotEqual(t, queued.PendingThreads[0].Generation, work[0].Generation, "selected ordinary work deliberately renews")
			for _, item := range work {
				require.Equal(t, "mcp", item.SourceName)
				require.Equal(t, "T1", item.WorkspaceID)
				require.Equal(t, "C1", item.ChannelID)
				require.NotEmpty(t, item.Generation)
			}
			selected := map[string]bool{threadWorkKey(work[0]): true, threadWorkKey(work[1]): true}
			require.Equal(t, mcpUnselectedSnapshot(t, before, selected), mcpUnselectedSnapshot(t, apiHistoryArchiveRows(t, st), selected),
				"only selected MCP jobs change; a pending job alone is not evidence")
		})
	}
}

func TestMCPReturnedThreadWorkSharesReplyGeneration(t *testing.T) {
	for _, nextMode := range []string{"ordinary", "same-since", "other-since-adapter"} {
		for _, completed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/complete=%t", nextMode, completed), func(t *testing.T) {
				ctx := context.Background()
				st, other := mcpHistoryWriteStores(t)
				now := time.Unix(1710000000, 0).UTC()
				seedBatchCatalog(t, st, "T1", "C1", "U1", now)
				root := batchMessage("C1", "1710000001.000000", "T1", "original", now)
				root.SourceName, root.SourceRank, root.ReplyCount = "mcp", 4, 1
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference", Since: "100"}
				history := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
				first, err := st.PrepareMCPReturnedThreadWork(ctx, history, []string{root.TS}, nil)
				require.NoError(t, err)
				require.Len(t, first, 1)
				if nextMode == "ordinary" {
					scope.Since = ""
				} else if nextMode == "other-since-adapter" {
					scope.Since, scope.Adapter = "200", "codex"
				}
				next := beginHistoryTest(t, other, scope, MCPHistoryOptions{})
				current, err := st.ThreadWorkCurrent(ctx, first[0])
				require.NoError(t, err)
				require.True(t, current, "history revision alone does not revoke independently acquired replies")
				done, err := other.CompleteMCPHistory(ctx, next, "")
				require.NoError(t, err)
				require.True(t, done)
				var second []ThreadWork
				if nextMode == "ordinary" {
					second, err = other.PrepareMCPThreadWork(ctx, next)
				} else {
					second, err = other.PrepareMCPReturnedThreadWork(ctx, next, []string{root.TS}, nil)
				}
				require.NoError(t, err)
				require.Len(t, second, 1)
				require.NotEqual(t, first[0].Generation, second[0].Generation)
				newer := root
				newer.Text, newer.NormalizedText = "newer winner", "newer winner"
				_, err = other.ApplyWriteBatch(ctx, WriteBatch{ThreadGuard: &second[0], Messages: []MessageWrite{{Message: newer}}})
				require.NoError(t, err)
				if completed {
					done, err := other.CompleteThreadWork(ctx, second[0], "", nil)
					require.NoError(t, err)
					require.True(t, done)
				}
				before := apiHistoryArchiveRows(t, st)
				written, err := st.ApplyWriteBatch(ctx, WriteBatch{ThreadGuard: &first[0], Messages: []MessageWrite{{Message: root}}})
				require.NoError(t, err)
				require.True(t, written.ThreadWorkRevoked)
				done, err = st.CompleteThreadWork(ctx, first[0], "", nil)
				require.NoError(t, err)
				require.False(t, done)
				require.Equal(t, before, apiHistoryArchiveRows(t, st), "a false old completion cannot retire or reacquire newer work")
				fresh, err := other.PrepareMCPReturnedThreadWork(ctx, next, []string{root.TS}, nil)
				require.NoError(t, err)
				require.Len(t, fresh, 1)
				require.NotEqual(t, second[0].Generation, fresh[0].Generation, "a later explicit acquisition remains possible")
			})
		}
	}
}

func mcpUnselectedSnapshot(t *testing.T, snapshot map[string][]string, selected map[string]bool) map[string][]string {
	t.Helper()
	result := make(map[string][]string, len(snapshot))
	for table, rows := range snapshot {
		result[table] = nil
		for _, raw := range rows {
			if table == "sync_state" {
				var row map[string]any
				require.NoError(t, json.Unmarshal([]byte(raw), &row))
				key, _ := row["entity_id"].(string)
				if row["source_name"] == "mcp" && row["entity_type"] == ThreadPendingEntityType && selected[key] {
					continue
				}
			}
			result[table] = append(result[table], raw)
		}
	}
	return result
}
