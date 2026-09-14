package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPIHistoryAttemptSupersedesAcrossHandles(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer-pending", true: "newer-complete"}[completed], func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "history.db")
			first, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, first.Close()) })
			second, err := Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, second.Close()) })
			seedBatchCatalog(t, first, "T1", "C1", "U1", time.Unix(1710000000, 0))
			scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
			a, err := first.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000000.000000")
			require.NoError(t, err)
			_, err = first.ApplyWriteBatch(ctx, WriteBatch{HistoryGuard: &a, Messages: []MessageWrite{{Message: batchMessage("C1", "1709990000.000000", "T1", "committed page", time.Unix(1710000000, 0))}}})
			require.NoError(t, err)
			b, err := second.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000000.000000")
			require.NoError(t, err)
			require.NotEmpty(t, a.Generation)
			require.NotEqual(t, a.Generation, b.Generation, "same clock does not identify an attempt")
			if completed {
				require.NoError(t, second.CompleteAPIHistory(ctx, b))
			}
			before := apiHistoryArchiveRows(t, first)
			require.ErrorIs(t, first.CheckAPIHistory(ctx, a), ErrAPIHistorySuperseded)
			require.ErrorIs(t, first.CompleteAPIHistory(ctx, a), ErrAPIHistorySuperseded)
			result, err := first.ApplyWriteBatch(ctx, WriteBatch{HistoryGuard: &a, Messages: []MessageWrite{{Message: batchMessage("C1", "1709999999.000000", "T1", "stale page", time.Unix(1710000000, 0))}}})
			require.ErrorIs(t, err, ErrAPIHistorySuperseded)
			require.Equal(t, WriteBatchResult{}, result)
			require.Equal(t, before, apiHistoryArchiveRows(t, first))
			if !completed {
				// Mutating returned request fields must not manufacture completion proof.
				b.Latest = "1999999999.000000"
				require.NoError(t, second.CompleteAPIHistory(ctx, b))
			}
			state, err := first.APIHistory(ctx, scope)
			require.NoError(t, err)
			require.Equal(t, APIHistoryState{Complete: true, Latest: "1710000000.000000"}, state)
			require.ErrorIs(t, second.CompleteAPIHistory(ctx, b), ErrAPIHistorySuperseded)
			assertBatchCount(t, first, "select count(*) from messages", 1)
		})
	}
}

func TestAPIHistoryBeginUsesCurrentBoundsAndRetention(t *testing.T) {
	for _, tc := range []struct {
		name, since, wantOldest string
		opts                    APIHistoryOptions
		enforce, inclusive      bool
	}{
		{"ordinary", "", "1709995000.000000", APIHistoryOptions{}, true, true},
		{"full", "", "", APIHistoryOptions{Full: true, RestoreRequested: true}, false, false},
		{"repair-full", "", "", APIHistoryOptions{Full: true}, true, false},
		{"since", "1709900000.000000", "1709900000.000000", APIHistoryOptions{RestoreRequested: true}, false, false},
		{"since-before-full", "1709900000.000000", "1709900000.000000", APIHistoryOptions{Full: true, RestoreRequested: true}, false, false},
		{"repair-since", "1709900000.000000", "1709900000.000000", APIHistoryOptions{}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			seedBatchCatalog(t, st, "T1", "C1", "U1", time.Unix(1710000000, 0))
			scope := APIHistoryScope{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", Since: tc.since}
			seedAPIHistoryState(t, st, scope, APIHistoryState{Complete: true, Latest: "1710000000.000000", Pending: new("1709800000.000000")})
			a, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{Full: true, RestoreRequested: true}, "1710100000.000000")
			require.NoError(t, err)
			// A floor installed after selection must govern the next ordinary request.
			require.NoError(t, st.SetSyncState(ctx, "retention", "channel_floor", "T1|C1", "1709995000.000000"))
			b, err := st.BeginAPIHistory(ctx, scope, tc.opts, "1709000000.000000")
			require.NoError(t, err)
			require.Equal(t, tc.wantOldest, b.Oldest)
			require.Equal(t, "1710100000.000000", b.Latest, "delayed now inherits the pending upper horizon")
			require.Equal(t, tc.enforce, b.EnforceRetention)
			require.Equal(t, tc.inclusive, b.Inclusive)
			state, err := st.APIHistory(ctx, scope)
			require.NoError(t, err)
			require.Equal(t, new(tc.wantOldest), state.Pending)
			require.Equal(t, b.Latest, state.PendingLatest)
			require.ErrorIs(t, st.CompleteAPIHistory(ctx, a), ErrAPIHistorySuperseded)
			require.NoError(t, st.CompleteAPIHistory(ctx, b))
			require.NoError(t, st.DeleteSyncState(ctx, "retention", "channel_floor", "T1|C1"))
			next, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1709000000.000000")
			require.NoError(t, err)
			require.Equal(t, b.Latest, next.Latest, "completed horizon also bounds a delayed request")
			want := tc.since
			if want == "" {
				want = "1710096400.000000"
			}
			require.Equal(t, want, next.Oldest)
		})
	}
}

func TestAPIHistoryAttemptScopeIsolation(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	for _, pair := range [][2]string{{"T1", "C1"}, {"T1", "C2"}, {"T2", "C3"}} {
		seedBatchCatalog(t, st, pair[0], pair[1], "U"+pair[1], time.Unix(1710000000, 0))
	}
	scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
	a, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000000.000000")
	require.NoError(t, err)
	for _, other := range []APIHistoryScope{
		{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1"},
		{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C2"},
		{SourceName: "api-bot", WorkspaceID: "T2", ChannelID: "C3"},
		{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1", Since: "1709900000.000000"},
	} {
		b, err := st.BeginAPIHistory(ctx, other, APIHistoryOptions{}, "1710000001.000000")
		require.NoError(t, err)
		require.NoError(t, st.CompleteAPIHistory(ctx, b))
		require.NoError(t, st.CheckAPIHistory(ctx, a))
	}
	before := apiHistoryArchiveRows(t, st)
	for _, bad := range []APIHistoryScope{
		{SourceName: "mcp", WorkspaceID: "T1", ChannelID: "C1"},
		{SourceName: "api-bot", WorkspaceID: "T2", ChannelID: "C1"},
		{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "absent"},
		{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1", Since: "NaN"},
	} {
		_, err := st.BeginAPIHistory(ctx, bad, APIHistoryOptions{}, "1710000000.000000")
		require.Error(t, err)
		require.Equal(t, before, apiHistoryArchiveRows(t, st))
	}
	require.NoError(t, st.CompleteAPIHistory(ctx, a))
}

func TestAPIHistorySelectedCheckpointValidation(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"legacy-empty", `{"complete":false,"latest":""}`, true},
		{"legacy-pending-empty", `{"complete":false,"latest":"","pending":""}`, true},
		{"legacy-complete", `{"complete":true,"latest":"1710000000.000000"}`, true},
		{"legacy-pending", `{"complete":true,"latest":"1710000000.000000","pending":"1709900000.000000"}`, true},
		{"invalid-json", `private-checkpoint-canary`, false},
		{"null", `null`, false},
		{"missing", `{}`, false},
		{"extra", `{"complete":false,"latest":"","private":"private-checkpoint-canary"}`, false},
		{"duplicate", `{"complete":false,"latest":"","complete":false}`, false},
		{"pending-null", `{"complete":false,"latest":"","pending":null}`, false},
		{"inconsistent-complete", `{"complete":true,"latest":""}`, false},
		{"latest-without-complete", `{"complete":false,"latest":"1710000000.000000"}`, false},
		{"nonfinite", `{"complete":true,"latest":"NaN"}`, false},
		{"bad-pending", `{"complete":false,"latest":"","pending":"private-checkpoint-canary"}`, false},
		{"generation-only", `{"complete":false,"latest":"","pending":"","generation":"g"}`, false},
		{"upper-only", `{"complete":false,"latest":"","pending":"","pending_latest":"1710000000.000000"}`, false},
		{"no-pending", `{"complete":false,"latest":"","generation":"g","pending_latest":"1710000000.000000"}`, false},
		{"upper-before-complete", `{"complete":true,"latest":"1710000001.000000","pending":"","generation":"g","pending_latest":"1710000000.000000"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openBatchTestStore(t)
			ctx := context.Background()
			seedBatchCatalog(t, st, "T1", "C1", "U1", time.Unix(1710000000, 0))
			require.NoError(t, st.SetSyncState(ctx, "api-bot", APIHistoryEntityType, `["T1","C1",""]`, tc.raw))
			// Unrelated malformed aliases are deliberately outside selected-key validation.
			require.NoError(t, st.SetSyncState(ctx, "api-bot", APIHistoryEntityType, "noncanonical-alias", "private-alias-canary"))
			before := apiHistoryArchiveRows(t, st)
			attempt, err := st.BeginAPIHistory(ctx, APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}, APIHistoryOptions{}, "1710000200.000000")
			if !tc.valid {
				require.EqualError(t, err, "invalid API history checkpoint")
				require.Equal(t, APIHistoryAttempt{}, attempt)
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, attempt.Generation)
			require.Equal(t, "1710000200.000000", attempt.Latest)
			require.NoError(t, st.CompleteAPIHistory(ctx, attempt))
			alias, err := st.GetSyncState(ctx, "api-bot", APIHistoryEntityType, "noncanonical-alias")
			require.NoError(t, err)
			require.Equal(t, "private-alias-canary", alias)
		})
	}
}

func TestAPIHistoryFencesThreadAndBatchTransactions(t *testing.T) {
	for _, completed := range []bool{false, true} {
		for _, operation := range []string{"prepare", "complete-thread", "message", "tombstone", "discovery", "skip-write", "skip-delete", "channel-state", "empty"} {
			t.Run(map[bool]string{false: "pending", true: "complete"}[completed]+"/"+operation, func(t *testing.T) {
				st := openBatchTestStore(t)
				ctx := context.Background()
				now := time.Unix(1710000000, 0)
				seedBatchCatalog(t, st, "T1", "C1", "U1", now)
				parent := batchMessage("C1", "1709990000.000000", "T1", "parent", now)
				parent.ReplyCount = 1
				require.NoError(t, st.UpsertMessage(ctx, parent, nil))
				scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
				a, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000000.000000")
				require.NoError(t, err)
				work, err := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", &a)
				require.NoError(t, err)
				require.Len(t, work, 1)
				skipKey := "T1|C1|" + parent.TS
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", skipKey, "missing_scope"))
				if operation == "prepare" {
					// Preparation must not reconcile this tombstone or renew any work when stale.
					_, err = st.DB().ExecContext(ctx, "update messages set deleted_ts='1710000000.000000'")
					require.NoError(t, err)
				}
				b, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000001.000000")
				require.NoError(t, err)
				if completed {
					require.NoError(t, st.CompleteAPIHistory(ctx, b))
				}
				before := apiHistoryArchiveRows(t, st)
				switch operation {
				case "prepare":
					result, e := st.PrepareThreadWork(ctx, "api-user", "T1", "C1", &a)
					err = e
					require.Empty(t, result)
				case "complete-thread":
					result, e := st.CompleteThreadWork(ctx, work[0], skipKey, &a)
					err = e
					require.False(t, result)
				default:
					batch := WriteBatch{HistoryGuard: &a}
					switch operation {
					case "message":
						batch.Messages = []MessageWrite{{Message: batchMessage("C1", "1709990001.000000", "T1", "stale", now)}}
					case "tombstone":
						parent.DeletedTS = "1710000001.000000"
						batch.Messages = []MessageWrite{{Message: parent}}
					case "discovery":
						batch.ThreadDiscovery = &ThreadWorkDiscovery{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1"}
						batch.PendingThreads = []ThreadWork{{SourceName: "api-user", WorkspaceID: "T1", ChannelID: "C1", TS: parent.TS}}
					case "skip-write":
						batch.SyncStates = []SyncStateWrite{{SourceName: "api-user", EntityType: "thread_skip", EntityID: skipKey, Value: "not_in_channel"}}
					case "skip-delete":
						batch.SyncStateDeletes = []SyncStateDelete{{SourceName: "api-user", EntityType: "thread_skip", EntityID: skipKey}}
					case "channel-state":
						batch.SyncStates = []SyncStateWrite{{SourceName: "api-bot", EntityType: "channel_join", EntityID: "C1", Value: "joined"}}
					}
					result, e := st.ApplyWriteBatch(ctx, batch)
					err = e
					require.Equal(t, WriteBatchResult{}, result)
				}
				require.ErrorIs(t, err, ErrAPIHistorySuperseded)
				require.Equal(t, before, apiHistoryArchiveRows(t, st), "all canonical, derived and state rows remain unchanged")
			})
		}
	}
}

func TestAPIHistoryCancellationAndRollback(t *testing.T) {
	st := openBatchTestStore(t)
	ctx := context.Background()
	seedBatchCatalog(t, st, "T1", "C1", "U1", time.Unix(1710000000, 0))
	scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
	a, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000000.000000")
	require.NoError(t, err)
	_, err = st.ApplyWriteBatch(ctx, WriteBatch{HistoryGuard: &a, Messages: []MessageWrite{{Message: batchMessage("C1", "1709990000.000000", "T1", "prior page", time.Unix(1710000000, 0))}}})
	require.NoError(t, err)
	before := apiHistoryArchiveRows(t, st)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = st.BeginAPIHistory(canceled, scope, APIHistoryOptions{}, "1710000100.000000")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, st.CompleteAPIHistory(canceled, a), context.Canceled)
	_, err = st.ApplyWriteBatch(canceled, WriteBatch{HistoryGuard: &a})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, apiHistoryArchiveRows(t, st))
	_, err = st.DB().ExecContext(ctx, `create trigger reject_history_update before update on sync_state when old.entity_type='history_coverage_v1' begin select raise(abort,'fixture history rollback'); end`)
	require.NoError(t, err)
	_, err = st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000100.000000")
	require.ErrorContains(t, err, "fixture history rollback")
	require.ErrorContains(t, st.CompleteAPIHistory(ctx, a), "fixture history rollback")
	require.Equal(t, before, apiHistoryArchiveRows(t, st))
	_, err = st.DB().ExecContext(ctx, `create trigger reject_skip_delete before delete on sync_state when old.entity_type='thread_skip' begin select raise(abort,'fixture skip rollback'); end`)
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|C1|1709990000.000000", "missing_scope"))
	before = apiHistoryArchiveRows(t, st)
	_, err = st.ApplyWriteBatch(ctx, WriteBatch{HistoryGuard: &a,
		Messages:         []MessageWrite{{Message: batchMessage("C1", "1709990001.000000", "T1", "rolled back", time.Unix(1710000000, 0))}},
		SyncStateDeletes: []SyncStateDelete{{SourceName: "api-user", EntityType: "thread_skip", EntityID: "T1|C1|1709990000.000000"}},
	})
	require.ErrorContains(t, err, "fixture skip rollback")
	require.Equal(t, before, apiHistoryArchiveRows(t, st))
	for _, horizon := range []string{"", "NaN", "+Inf", "private-horizon-canary"} {
		_, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, horizon)
		require.EqualError(t, err, "API history requires a finite timestamp")
		require.Equal(t, before, apiHistoryArchiveRows(t, st))
	}
}

func seedAPIHistoryState(t *testing.T, st *Store, scope APIHistoryScope, state APIHistoryState) {
	t.Helper()
	key, err := json.Marshal([3]string{scope.WorkspaceID, scope.ChannelID, scope.Since})
	require.NoError(t, err)
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(context.Background(), scope.SourceName, APIHistoryEntityType, string(key), string(raw)))
}

func apiHistoryArchiveRows(t *testing.T, st *Store) map[string][]string {
	t.Helper()
	snapshot := map[string][]string{}
	for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "sync_state", "message_mentions", "embedding_jobs", "message_fts"} {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		for _, row := range rows {
			raw, err := json.Marshal(row)
			require.NoError(t, err)
			snapshot[table] = append(snapshot[table], string(raw))
		}
		sort.Strings(snapshot[table])
	}
	return snapshot
}
