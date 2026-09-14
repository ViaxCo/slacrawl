package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPIThreadCoverageStatusRecoversHistoricalMarker(t *testing.T) {
	for _, source := range []string{"api-bot", "api-user"} {
		for _, marker := range []string{"full", "partial", "", "stored-status"} {
			t.Run(source+"/"+marker, func(t *testing.T) {
				ctx := context.Background()
				st := openBatchTestStore(t)
				seedBatchCatalog(t, st, "T1", "C1", "U1", time.Unix(1710000000, 0))
				if marker != "" {
					seedAPIThreadCoverageMarker(t, st, marker)
				}
				before := apiThreadCoverageMarker(t, st)
				scope := APIHistoryScope{SourceName: source, WorkspaceID: "T1", ChannelID: "C1"}
				attempt, err := st.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000100.000000")
				require.NoError(t, err)
				status, facts, err := st.StatusWithAPIThreadCoverage(ctx)
				require.NoError(t, err)
				want := marker
				if marker == "" || marker == "full" {
					want = "partial"
				}
				require.Equal(t, want, status.ThreadState)
				require.Equal(t, APIThreadCoverageFacts{IncompleteHistory: true}, facts)
				require.NoError(t, st.PublishAPIThreadCoverage(ctx, APIThreadCoveragePublication{FullEligible: true}))
				require.Equal(t, before, apiThreadCoverageMarker(t, st), "temporary retained work must not rewrite the historical marker or time")
				require.NoError(t, st.CompleteAPIHistory(ctx, attempt))
				status, facts, err = st.StatusWithAPIThreadCoverage(ctx)
				require.NoError(t, err)
				want = marker
				if want == "" {
					want = "partial"
				}
				require.Equal(t, want, status.ThreadState, "clearing work must not promote a genuine partial or absent marker")
				require.Equal(t, APIThreadCoverageFacts{}, facts)
				require.Equal(t, before, apiThreadCoverageMarker(t, st))
			})
		}
	}
}

func TestAPIThreadCoverageFactsUseExactOwners(t *testing.T) {
	for _, tc := range []struct {
		name, source, kind string
		thread, history    bool
	}{
		{"skip", "api-user", "thread_skip", true, false},
		{"pending", "api-user", ThreadPendingEntityType, true, false},
		{"bot-history", "api-bot", APIHistoryEntityType, false, true},
		{"user-history", "api-user", APIHistoryEntityType, false, true},
		{"mcp-job", "mcp", ThreadPendingEntityType, false, false},
		{"mcp-history", "mcp", APIHistoryEntityType, false, false},
		{"bot-job", "api-bot", ThreadPendingEntityType, false, false},
		{"other-type", "api-user", "other", false, false},
		{"future-history", "api-user", "history_coverage_v2", false, false},
		{"near-source", "API-USER", APIHistoryEntityType, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			seedAPIThreadCoverageMarker(t, st, "full")
			key, value := "opaque", "private-ignored-value"
			if tc.history {
				key, value = `["T1","C1",""]`, `{"complete":false,"latest":""}`
			}
			require.NoError(t, st.SetSyncState(ctx, tc.source, tc.kind, key, value))
			before := apiHistoryArchiveRows(t, st)
			status, facts, err := st.StatusWithAPIThreadCoverage(ctx)
			require.NoError(t, err)
			require.Equal(t, APIThreadCoverageFacts{ThreadWork: tc.thread, IncompleteHistory: tc.history}, facts)
			want := "full"
			if tc.thread || tc.history {
				want = "partial"
			}
			require.Equal(t, want, status.ThreadState)
			ordinary, err := st.Status(ctx)
			require.NoError(t, err)
			require.Equal(t, status, ordinary)
			require.Equal(t, before, apiHistoryArchiveRows(t, st))
		})
	}
}

func TestAPIThreadCoverageReadValidationIsLazy(t *testing.T) {
	for _, marker := range []string{"full", "partial", "stored-status", ""} {
		for _, mode := range []string{"key", "value", "late-value"} {
			t.Run(marker+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				st := openBatchTestStore(t)
				if marker != "" {
					seedAPIThreadCoverageMarker(t, st, marker)
				}
				key, raw := `["TZ","CZ",""]`, "private-history-canary"
				if mode == "key" {
					key = "private-key-canary"
				}
				if mode == "late-value" {
					seedAPIHistoryState(t, st, APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T0", ChannelID: "C0"}, APIHistoryState{Pending: new("")})
				}
				require.NoError(t, st.SetSyncState(ctx, "api-user", APIHistoryEntityType, key, raw))
				before := apiHistoryArchiveRows(t, st)
				wantError := "invalid API history checkpoint"
				if mode == "key" {
					wantError += " key"
				}
				status, err := st.Status(ctx)
				if marker == "full" {
					require.EqualError(t, err, wantError)
					require.Equal(t, Status{}, status)
				} else {
					require.NoError(t, err)
					want := marker
					if want == "" {
						want = "partial"
					}
					require.Equal(t, want, status.ThreadState)
				}
				status, facts, err := st.StatusWithAPIThreadCoverage(ctx)
				require.EqualError(t, err, wantError)
				require.Equal(t, Status{}, status)
				require.Equal(t, APIThreadCoverageFacts{}, facts)
				for _, eligible := range []bool{false, true} {
					require.EqualError(t, st.PublishAPIThreadCoverage(ctx, APIThreadCoveragePublication{FullEligible: eligible}), wantError)
				}
				require.Equal(t, before, apiHistoryArchiveRows(t, st))
			})
		}
	}
}

func TestAPIThreadCoveragePublicationIsAtomic(t *testing.T) {
	for _, mode := range []string{"clear", "local-history", "foreign-history", "local-thread", "foreign-thread", "foreign-skip", "partial", "no-cleanup", "foreign-invalid", "write-failure", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := openBatchTestStore(t)
			seedAPIThreadCoverageMarker(t, st, "full")
			require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T1|legacy", "local"))
			publication := APIThreadCoveragePublication{FullEligible: true, CleanupWorkspaceID: "T1"}
			switch mode {
			case "local-history", "foreign-history":
				workspace := "T1"
				if mode == "foreign-history" {
					workspace = "T2"
				}
				seedAPIHistoryState(t, st, APIHistoryScope{SourceName: "api-bot", WorkspaceID: workspace, ChannelID: "C1"}, APIHistoryState{Pending: new("")})
			case "local-thread", "foreign-thread":
				key := `["T1","C1","1"]`
				if mode == "foreign-thread" {
					key = `["T2","C2","1"]`
				}
				require.NoError(t, st.SetSyncState(ctx, "api-user", ThreadPendingEntityType, key, "generation"))
			case "foreign-skip":
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "T2|legacy", "foreign"))
			case "partial":
				publication.FullEligible = false
			case "no-cleanup":
				publication.CleanupWorkspaceID = ""
			case "foreign-invalid":
				require.NoError(t, st.SetSyncState(ctx, "api-user", APIHistoryEntityType, `["T2","C2",""]`, "private-value-canary"))
			case "write-failure":
				_, err := st.DB().ExecContext(ctx, `create trigger reject_coverage before insert on sync_state
when new.source_name='doctor' and new.entity_type='threads'
begin select raise(abort,'synthetic_publication_failure'); end`)
				require.NoError(t, err)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := apiHistoryArchiveRows(t, st)
			marker := apiThreadCoverageMarker(t, st)
			err := st.PublishAPIThreadCoverage(ctx, publication)
			switch mode {
			case "foreign-invalid":
				require.EqualError(t, err, "invalid API history checkpoint")
			case "write-failure":
				require.ErrorContains(t, err, "synthetic_publication_failure")
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.NoError(t, err)
			}
			if err != nil {
				require.Equal(t, before, apiHistoryArchiveRows(t, st), "cleanup and marker roll back together")
				return
			}
			local, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-user' and entity_type='thread_skip' and entity_id='T1|legacy'")
			require.NoError(t, err)
			keepsLocal := mode == "local-history" || mode == "local-thread" || mode == "partial" || mode == "no-cleanup"
			require.Equal(t, keepsLocal, len(local) == 1)
			if mode == "clear" || mode == "partial" {
				value, err := st.GetSyncState(ctx, "doctor", "threads", "coverage")
				require.NoError(t, err)
				require.Equal(t, map[bool]string{true: "partial", false: "full"}[mode == "partial"], value)
				require.NotEqual(t, marker, apiThreadCoverageMarker(t, st))
			} else {
				require.Equal(t, marker, apiThreadCoverageMarker(t, st), "durable blockers retain both historical value and timestamp")
			}
			status, err := st.Status(ctx)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{true: "full", false: "partial"}[mode == "clear"], status.ThreadState)
		})
	}
}

func TestAPIThreadCoverageStatusUsesReadSnapshot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	writer, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	seedBatchCatalog(t, writer, "T1", "C1", "U1", time.Unix(1710000000, 0))
	seedAPIThreadCoverageMarker(t, writer, "full")
	require.NoError(t, writer.SetSyncState(ctx, "api-bot", "workspace", "T1", "old"))
	reader, err := OpenReadOnly(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	initial, err := reader.Status(ctx)
	require.NoError(t, err)
	tx, err := reader.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	// Establish the actual SQLite read snapshot before the second handle writes.
	before, beforeFacts, err := readStatus(ctx, tx, true)
	require.NoError(t, err)
	require.Equal(t, initial, before)
	require.Equal(t, APIThreadCoverageFacts{}, beforeFacts)
	scope := APIHistoryScope{SourceName: "api-bot", WorkspaceID: "T1", ChannelID: "C1"}
	attempt, err := writer.BeginAPIHistory(ctx, scope, APIHistoryOptions{}, "1710000100.000000")
	require.NoError(t, err)
	require.NoError(t, writer.UpsertMessage(ctx, batchMessage("C1", "1710000001.000000", "T1", "new message", time.Unix(1710000001, 0)), nil))
	require.NoError(t, writer.SetSyncState(ctx, "api-bot", "workspace", "T1", "new"))
	_, err = writer.DB().ExecContext(ctx, "update sync_state set updated_at='2099-01-01T00:00:00Z' where source_name='api-bot' and entity_type='workspace'")
	require.NoError(t, err)
	same, sameFacts, err := readStatus(ctx, tx, true)
	require.NoError(t, err)
	require.Equal(t, before, same)
	require.Equal(t, beforeFacts, sameFacts)
	require.NoError(t, tx.Commit())
	fresh, facts, err := reader.StatusWithAPIThreadCoverage(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Messages+1, fresh.Messages)
	require.Equal(t, before.Workspaces, fresh.Workspaces)
	require.Equal(t, before.Channels, fresh.Channels)
	require.Equal(t, before.Users, fresh.Users)
	require.Equal(t, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), fresh.LastSyncAt)
	require.Equal(t, "partial", fresh.ThreadState)
	require.Equal(t, APIThreadCoverageFacts{IncompleteHistory: true}, facts)
	require.NoError(t, writer.CompleteAPIHistory(ctx, attempt))
	recovered, err := reader.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "full", recovered.ThreadState)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	zero, facts, err := reader.StatusWithAPIThreadCoverage(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Status{}, zero)
	require.Equal(t, APIThreadCoverageFacts{}, facts)
}

func seedAPIThreadCoverageMarker(t *testing.T, st *Store, value string) {
	t.Helper()
	_, err := st.DB().ExecContext(context.Background(), "insert into sync_state values ('doctor','threads','coverage',?,'2000-01-01T00:00:00Z')", value)
	require.NoError(t, err)
}

func apiThreadCoverageMarker(t *testing.T, st *Store) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), "select value,updated_at from sync_state where source_name='doctor' and entity_type='threads' and entity_id='coverage'")
	require.NoError(t, err)
	return rows
}
