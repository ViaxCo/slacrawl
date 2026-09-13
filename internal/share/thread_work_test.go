package share

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestThreadWorkStaysLocal(t *testing.T) {
	ctx := context.Background()
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{true: "restore", false: "merge"}[restore], func(t *testing.T) {
			source := seedStore(t, filepath.Join(t.TempDir(), "source.db"))
			defer func() { require.NoError(t, source.Close()) }()
			for _, name := range []string{"api-user", "mcp", "api-bot"} {
				require.NoError(t, source.SetSyncState(ctx, name, "thread_pending_v1", "opaque", "foreign"))
			}
			opts := Options{RepoPath: filepath.Join(t.TempDir(), "share")}
			manifest, err := Export(ctx, source, opts)
			require.NoError(t, err)
			for _, table := range manifest.Tables {
				if table.Name != "sync_state" {
					continue
				}
				rows := historySnapshotRows(t, opts.RepoPath, table)
				foundUnrelated := false
				for _, row := range rows {
					if row["entity_type"] == "thread_pending_v1" {
						require.Equal(t, "api-bot", row["source_name"])
						foundUnrelated = true
					}
				}
				require.True(t, foundUnrelated, "filter only the two owning sources")
			}
			incoming := []map[string]any{
				historySnapshotRow("api-user", "thread_pending_v1", "same", "foreign"),
				historySnapshotRow("mcp", "thread_pending_v1", "new", "foreign"),
				historySnapshotRow("api-bot", "thread_pending_v1", "other", "keep"),
			}
			writeHistorySnapshot(t, opts.RepoPath, &manifest, incoming, false)
			reader := seedStore(t, filepath.Join(t.TempDir(), "reader.db"))
			defer func() { require.NoError(t, reader.Close()) }()
			require.NoError(t, reader.SetSyncState(ctx, "api-user", "thread_pending_v1", "same", "local"))
			before, err := reader.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			importer := Import
			if restore {
				importer = Restore
			}
			// Even discarded local progress must satisfy row validation, and
			// a failed restore cannot remove the receiver's existing work.
			incoming[1]["value"] = map[string]any{"invalid": true}
			writeHistorySnapshot(t, opts.RepoPath, &manifest, incoming, false)
			_, err = importer(ctx, reader, opts)
			require.Error(t, err)
			after, err := reader.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			require.Equal(t, before, after)
			incoming[1]["value"] = "foreign"
			writeHistorySnapshot(t, opts.RepoPath, &manifest, incoming, false)
			_, err = importer(ctx, reader, opts)
			require.NoError(t, err)
			owned, err := reader.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1' and source_name in ('api-user','mcp')")
			require.NoError(t, err)
			if restore {
				require.Empty(t, owned)
			} else {
				require.Equal(t, before, owned)
			}
			value, err := reader.GetSyncState(ctx, "api-bot", "thread_pending_v1", "other")
			require.NoError(t, err)
			require.Equal(t, "keep", value)
		})
	}
}
