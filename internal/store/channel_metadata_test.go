package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelMetadataUpdatesRequireWorkspace(t *testing.T) {
	for _, operation := range []string{"rename", "archive", "unarchive"} {
		t.Run(operation, func(t *testing.T) {
			st, err := Open(filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer st.Close()
			ctx := context.Background()
			require.NoError(t, st.UpsertChannel(ctx, Channel{ID: "C123", WorkspaceID: "T123", Name: "original", IsArchived: operation == "unarchive", UpdatedAt: time.Unix(1710000000, 0)}))
			before, err := st.QueryReadOnly(ctx, "select * from channels")
			require.NoError(t, err)
			update := func(workspace, channel string) error {
				if operation == "rename" {
					return st.RenameChannel(ctx, workspace, channel, "updated")
				}
				return st.SetChannelArchived(ctx, workspace, channel, operation == "archive")
			}
			for _, target := range [][2]string{{"TOTHER", "C123"}, {"", "C123"}, {"T123", "CMISSING"}} {
				require.NoError(t, update(target[0], target[1]))
				after, err := st.QueryReadOnly(ctx, "select * from channels")
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
			require.NoError(t, update("T123", "C123"))
			after, err := st.QueryReadOnly(ctx, "select * from channels")
			require.NoError(t, err)
			require.NotEqual(t, before, after)
			if operation == "rename" {
				require.Equal(t, "updated", after[0]["name"])
			} else {
				require.Equal(t, boolInt(operation == "archive"), after[0]["is_archived"])
			}
		})
	}
}
