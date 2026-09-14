package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
	"github.com/stretchr/testify/require"
)

func TestMCPHistoryAttemptOwnership(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.db")
	first, err := Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
	original := beginHistoryTest(t, first, scope, MCPHistoryOptions{})
	require.Empty(t, original.Oldest)
	completed, err := first.CompleteMCPHistory(ctx, original, "1710000000.000001")
	require.NoError(t, err)
	require.True(t, completed)
	// A second Store must use the intervening committed interval rather than
	// the earlier completed scan. HTTP overlap is covered by the MCP Sync test.
	q, commit, rollback, err := first.beginMessageTransaction(ctx, true)
	require.NoError(t, err)
	defer rollback()
	key, err := scope.key()
	require.NoError(t, err)
	pending := "1709990000.000000"
	require.NoError(t, saveMCPHistory(ctx, storedb.New(q), key, MCPHistoryState{Complete: true, Latest: "1710090000.000000", Pending: &pending, Revision: "intervening-owner"}))
	require.NoError(t, commit())
	next, selected, err := second.BeginMCPHistory(ctx, scope, MCPHistoryOptions{})
	require.NoError(t, err)
	require.True(t, selected)
	require.Equal(t, pending, next.Oldest)
	require.NotEqual(t, "intervening-owner", next.Revision)
	require.Equal(t, MCPHistoryState{Complete: true, Latest: "1710090000.000000", Pending: &pending, Revision: next.Revision}, readHistoryTest(t, first, scope))
	before := historyStateRows(t, first)
	completed, err = first.CompleteMCPHistory(ctx, original, "1710990000.000000")
	require.NoError(t, err)
	require.False(t, completed)
	require.Equal(t, before, historyStateRows(t, first))
	completed, err = second.CompleteMCPHistory(ctx, next, "1710090010.000000")
	require.NoError(t, err)
	require.True(t, completed)
	before = historyStateRows(t, first)
	completed, err = second.CompleteMCPHistory(ctx, next, "1710990000.000000")
	require.NoError(t, err)
	require.False(t, completed)
	require.Equal(t, before, historyStateRows(t, first), "repeated completion is not a new scan")
}

func TestMCPHistoryCompletionKeepsReturnedWatermark(t *testing.T) {
	st := openBatchTestStore(t)
	scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "codex"}
	for _, latest := range []string{"", "9.000000", "10.000001", "9.000001", "", "1.1e1"} {
		work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
		prior := readHistoryTest(t, st, scope)
		completed, err := st.CompleteMCPHistory(context.Background(), work, latest)
		require.NoError(t, err)
		require.True(t, completed)
		state := readHistoryTest(t, st, scope)
		require.True(t, state.Complete)
		require.Nil(t, state.Pending)
		require.Equal(t, work.Revision, state.Revision)
		expected := latest
		if latest == "" || latest == "9.000001" {
			expected = prior.Latest
		}
		require.Equal(t, expected, state.Latest)
	}
}

func TestMCPHistoryScopeAndRetention(t *testing.T) {
	ctx := context.Background()
	st := openBatchTestStore(t)
	scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "codex"}
	ordinary := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
	completed, err := st.CompleteMCPHistory(ctx, ordinary, "1710000000.000000")
	require.NoError(t, err)
	require.True(t, completed)
	ordinary = beginHistoryTest(t, st, scope, MCPHistoryOptions{})
	require.Equal(t, "1709996400.000000", ordinary.Oldest)
	// The logical unfinished interval survives floor changes and Full's one-call authority.
	for _, floor := range []string{"1710000000.000000", "1710000100.000000"} {
		require.NoError(t, st.SetSyncState(ctx, "retention", "channel_floor", "T1|C1", floor))
		retry := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
		require.Equal(t, previousMicrosecondTimestamp(floor), retry.Oldest)
		require.True(t, retry.EnforceRetention)
		require.Equal(t, new("1709996400.000000"), readHistoryTest(t, st, scope).Pending)
	}
	full := beginHistoryTest(t, st, scope, MCPHistoryOptions{Full: true})
	require.Empty(t, full.Oldest)
	require.False(t, full.EnforceRetention)
	retry := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
	require.Equal(t, "1710000099.999999", retry.Oldest)
	require.True(t, retry.EnforceRetention)
	require.Equal(t, new(""), readHistoryTest(t, st, scope).Pending)
	before := readHistoryTest(t, st, scope)
	for _, isolated := range []MCPHistoryScope{
		{"T2", "C1", "codex", ""}, {"T1", "C2", "codex", ""}, {"T1", "C1", "reference", ""}, {"T1", "C1", "codex", "1709900000.000000"},
	} {
		work := beginHistoryTest(t, st, isolated, MCPHistoryOptions{Full: true})
		require.Equal(t, isolated.Since, work.Oldest)
		require.False(t, work.EnforceRetention)
		completed, err := st.CompleteMCPHistory(ctx, work, "1710090000.000000")
		require.NoError(t, err)
		require.True(t, completed)
		require.Equal(t, before, readHistoryTest(t, st, scope))
	}
	// Exact source/type ownership leaves opaque sibling progress untouched.
	key, err := scope.key()
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(ctx, "api-user", MCPHistoryEntityType, key, "foreign source"))
	require.NoError(t, st.SetSyncState(ctx, "mcp", "history_coverage_v1", key, "opaque old type"))
	beginHistoryTest(t, st, scope, MCPHistoryOptions{})
	value, err := st.GetSyncState(ctx, "api-user", MCPHistoryEntityType, key)
	require.NoError(t, err)
	require.Equal(t, "foreign source", value)
	value, err = st.GetSyncState(ctx, "mcp", "history_coverage_v1", key)
	require.NoError(t, err)
	require.Equal(t, "opaque old type", value)
	require.Equal(t, "1772577698.999999", previousMicrosecondTimestamp("1772577699.000000"))
	require.Equal(t, "1772577699.000099", previousMicrosecondTimestamp("1772577699.000100"))
	require.Equal(t, "invalid", previousMicrosecondTimestamp("invalid"))
}

func TestMCPHistoryLatestOnlyPreservesEligibility(t *testing.T) {
	for _, mode := range []string{"empty", "completed-empty", "pending-only", "malformed-unused", "draft-only", "orphan", "message", "retention-seed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
			if mode != "orphan" {
				seedBatchCatalog(t, st, "T1", "C1", "U1", time.Unix(1, 0).UTC())
			}
			switch mode {
			case "completed-empty", "pending-only":
				work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
				if mode == "completed-empty" {
					ok, err := st.CompleteMCPHistory(ctx, work, "")
					require.NoError(t, err)
					require.True(t, ok)
				}
			case "malformed-unused":
				key, err := scope.key()
				require.NoError(t, err)
				require.NoError(t, st.SetSyncState(ctx, "mcp", MCPHistoryEntityType, key, "invalid"))
			case "draft-only", "message", "orphan":
				ts := "1710000000.000000"
				if mode == "draft-only" {
					ts = "draft:synthetic"
				}
				require.NoError(t, st.UpsertMessage(ctx, batchMessage("C1", ts, "T1", "seed", time.Unix(1, 0).UTC()), nil))
			case "retention-seed":
				require.NoError(t, st.SetSyncState(ctx, "retention", "channel_seed", "T1|C1", "seed"))
			}
			before := historyStateRows(t, st)
			work, selected, err := st.BeginMCPHistory(ctx, scope, MCPHistoryOptions{LatestOnly: true})
			require.NoError(t, err)
			require.Equal(t, mode == "message" || mode == "retention-seed", selected)
			if selected {
				require.Empty(t, work.Oldest, "eligible messages must not supply a history cutoff")
			} else {
				require.Equal(t, before, historyStateRows(t, st))
			}
		})
	}
}

func TestMCPHistoryOwnershipAndRollback(t *testing.T) {
	for _, mode := range []string{"foreign", "begin-write", "complete-write", "missing-work", "invalid-state", "invalid-scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "reference"}
			if mode == "foreign" {
				seedBatchCatalog(t, st, "T2", "C1", "U2", time.Unix(1, 0).UTC())
			}
			work := MCPHistoryWork{MCPHistoryScope: scope, Revision: "missing"}
			if mode == "complete-write" {
				work = beginHistoryTest(t, st, scope, MCPHistoryOptions{})
			}
			if mode == "begin-write" || mode == "complete-write" {
				operation := "insert"
				if mode == "complete-write" {
					operation = "update"
				}
				_, err := st.DB().ExecContext(ctx, "create trigger fail_history before "+operation+" on sync_state when new.entity_type='history_work_v1' begin select raise(abort,'synthetic history write'); end")
				require.NoError(t, err)
			}
			if mode == "invalid-state" {
				key, err := scope.key()
				require.NoError(t, err)
				require.NoError(t, st.SetSyncState(ctx, "mcp", MCPHistoryEntityType, key, `{"revision":"broken","complete":true,"latest":"NaN"}`))
			}
			if mode == "invalid-scope" {
				scope.Adapter = "unknown"
			}
			before := historyStateRows(t, st)
			if mode == "complete-write" || mode == "missing-work" {
				done, err := st.CompleteMCPHistory(ctx, work, "1710000000.000000")
				require.False(t, done)
				if mode == "missing-work" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "synthetic history write")
				}
			} else {
				_, selected, err := st.BeginMCPHistory(ctx, scope, MCPHistoryOptions{})
				require.False(t, selected)
				require.Error(t, err)
				if mode == "foreign" {
					require.True(t, IsWorkspaceCollision(err, "channel"))
				}
			}
			require.Equal(t, before, historyStateRows(t, st))
		})
	}
}

func TestMCPHistoryProgressDoesNotAdvanceFreshness(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	scope := MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "codex"}
	work := beginHistoryTest(t, st, scope, MCPHistoryOptions{})
	status, err := st.Status(ctx)
	require.NoError(t, err)
	require.True(t, status.LastSyncAt.IsZero())
	done, err := st.CompleteMCPHistory(ctx, work, "1710000000.000000")
	require.NoError(t, err)
	require.True(t, done)
	status, err = st.Status(ctx)
	require.NoError(t, err)
	require.True(t, status.LastSyncAt.IsZero())
	for _, source := range []string{"api-user", "api-bot", "mcp"} {
		require.NoError(t, st.SetSyncState(ctx, source, "history_coverage_v1", "opaque", "preserved"))
		status, err = st.Status(ctx)
		require.NoError(t, err)
		require.False(t, status.LastSyncAt.IsZero(), "existing history types retain their freshness eligibility")
		require.NoError(t, st.DeleteSyncState(ctx, source, "history_coverage_v1", "opaque"))
	}
}

func beginHistoryTest(t *testing.T, st *Store, scope MCPHistoryScope, opts MCPHistoryOptions) MCPHistoryWork {
	t.Helper()
	work, selected, err := st.BeginMCPHistory(context.Background(), scope, opts)
	require.NoError(t, err)
	require.True(t, selected)
	require.NotEmpty(t, work.Revision)
	return work
}

func readHistoryTest(t *testing.T, st *Store, scope MCPHistoryScope) MCPHistoryState {
	t.Helper()
	key, err := scope.key()
	require.NoError(t, err)
	state, err := loadMCPHistory(context.Background(), st.q, key)
	require.NoError(t, err)
	return state
}

func historyStateRows(t *testing.T, st *Store) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), "select * from sync_state order by source_name,entity_type,entity_id")
	require.NoError(t, err)
	return rows
}
