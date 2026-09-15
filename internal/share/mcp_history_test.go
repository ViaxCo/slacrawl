package share

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMCPHistoryWorkStaysLocal(t *testing.T) {
	ctx := context.Background()
	scope := store.MCPHistoryScope{WorkspaceID: "T1", ChannelID: "C1", Adapter: "codex"}
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "merge", true: "restore"}[restore], func(t *testing.T) {
			source := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { require.NoError(t, source.Close()) }()
			work, selected, err := source.BeginMCPHistory(ctx, scope, store.MCPHistoryOptions{})
			require.NoError(t, err)
			require.True(t, selected)
			done, err := source.CompleteMCPHistory(ctx, work, "1710000000.000000")
			require.NoError(t, err)
			require.True(t, done)
			for _, row := range []struct{ source, kind, value string }{
				{"mcp", "history_coverage_v1", "opaque MCP compatibility"},
				{"api-user", store.MCPHistoryEntityType, "other source"},
			} {
				require.NoError(t, source.SetSyncState(ctx, row.source, row.kind, "opaque", row.value))
			}
			opts := Options{RepoPath: filepath.Join(t.TempDir(), "share")}
			manifest, err := Export(ctx, source, opts)
			require.NoError(t, err)
			var exported []map[string]any
			for _, table := range manifest.Tables {
				if table.Name == "sync_state" {
					exported = historySnapshotRows(t, opts.RepoPath, table)
				}
			}
			require.Len(t, exported, 3, "workspace and two unrelated controls remain shareable")
			for _, row := range exported {
				require.False(t, row["source_name"] == "mcp" && row["entity_type"] == store.MCPHistoryEntityType)
			}
			incoming := append(exported, historySnapshotRow("mcp", store.MCPHistoryEntityType, `["T1","C1","codex",""]`, "foreign opaque progress"))
			reader := seedStore(t, filepath.Join(t.TempDir(), "reader.db"))
			defer func() { require.NoError(t, reader.Close()) }()
			local, selected, err := reader.BeginMCPHistory(ctx, scope, store.MCPHistoryOptions{})
			require.NoError(t, err)
			require.True(t, selected)
			done, err = reader.CompleteMCPHistory(ctx, local, "1710000000.000000")
			require.NoError(t, err)
			require.True(t, done)
			before := historyArchiveState(t, reader)
			importer := Import
			if restore {
				importer = Restore
			}
			// Even discarded progress must pass row admission. A failed Restore must
			// preserve the local checkpoint and every existing materialized table.
			for _, field := range []string{"entity_id", "value", "updated_at"} {
				row := incoming[len(incoming)-1]
				saved := row[field]
				row[field] = map[string]any{"invalid": true}
				writeHistorySnapshot(t, opts.RepoPath, &manifest, incoming, false)
				_, err = importer(ctx, reader, opts)
				require.Error(t, err)
				require.Equal(t, before, historyArchiveState(t, reader))
				row[field] = saved
			}
			writeHistorySnapshot(t, opts.RepoPath, &manifest, incoming, false)
			_, err = importer(ctx, reader, opts)
			require.NoError(t, err)
			rows, err := reader.QueryReadOnly(ctx, "select * from sync_state where source_name='mcp' and entity_type='history_work_v1'")
			require.NoError(t, err)
			if restore {
				require.Empty(t, rows)
			} else {
				var localRows []map[string]any
				for _, row := range before["sync_state"] {
					if row["entity_type"] == store.MCPHistoryEntityType && row["source_name"] == "mcp" {
						localRows = append(localRows, row)
					}
				}
				require.Equal(t, localRows, rows)
			}
			value, err := reader.GetSyncState(ctx, "mcp", "history_coverage_v1", "opaque")
			require.NoError(t, err)
			require.Equal(t, "opaque MCP compatibility", value)
			value, err = reader.GetSyncState(ctx, "api-user", store.MCPHistoryEntityType, "opaque")
			require.NoError(t, err)
			require.Equal(t, "other source", value)
			next, selected, err := reader.BeginMCPHistory(ctx, scope, store.MCPHistoryOptions{})
			require.NoError(t, err)
			require.True(t, selected)
			expected := "1709996400.000000"
			if restore {
				expected = ""
			}
			require.Equal(t, expected, next.Oldest, "Restore must establish fresh local coverage instead of using restored messages")
		})
	}
}
