package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/stretchr/testify/require"
)

func TestSyncExcludePrecedesCheckpointAccess(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full=%t", full), func(t *testing.T) {
			st := openTestStore(t)
			require.NoError(t, st.Close())
			requestPath := filepath.Join(t.TempDir(), "request.json")
			summary, err := Sync(context.Background(), st, helperProvider(t, "success", requestPath), Options{
				DMPolicy: admission.Exclude, WorkspaceID: "T1", Full: full,
			})
			require.EqualError(t, err, "external provider v1 cannot enforce sync.include_dms=false; use API sync or a supported Slack workspace JSON export")
			require.Equal(t, Summary{}, summary)
			_, err = os.Stat(requestPath)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestSyncDMPolicyTransitionPreservesCursors(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("scoped=%t", scoped), func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			opts := Options{DMPolicy: admission.Include, WorkspaceID: "T1"}
			normalKey := checkpointStateKey{entityType: "workspace", entityID: "T1"}
			scopedOpts := Options{WorkspaceID: "T1", Since: "1.000000", Channels: []string{"C1"}}
			scopedKey := stateKey("T1", scopedOpts)
			require.Equal(t, checkpointStateKey{entityType: "workspace_scope", entityID: "T1|af45c1397b5a22341080cefa21c2271f"}, scopedKey)
			if scoped {
				opts.Since, opts.Channels = scopedOpts.Since, scopedOpts.Channels
			}
			selected, other := normalKey, scopedKey
			selectedSeed, otherSeed := "normal-before", "scoped-before"
			if scoped {
				selected, other = other, selected
				selectedSeed, otherSeed = otherSeed, selectedSeed
			}
			require.Equal(t, selected, stateKey("T1", opts))
			for _, policy := range []admission.DMPolicy{admission.Default, admission.Exclude, admission.Include} {
				withPolicy := opts
				withPolicy.DMPolicy = policy
				require.Equal(t, selected, stateKey("T1", withPolicy))
			}
			require.NoError(t, st.SetSyncState(ctx, "provider:fixture", normalKey.entityType, normalKey.entityID, "normal-before"))
			require.NoError(t, st.SetSyncState(ctx, "provider:fixture", scopedKey.entityType, scopedKey.entityID, "scoped-before"))
			firstPath := filepath.Join(t.TempDir(), "first.json")
			_, err := Sync(ctx, st, helperProvider(t, "success", firstPath), opts)
			require.NoError(t, err)
			var first request
			require.NoError(t, json.Unmarshal(requireReadFile(t, firstPath), &first))
			require.Equal(t, selectedSeed, first.Checkpoint)
			require.Equal(t, opts.Since, first.Since)
			require.Equal(t, opts.Channels, first.Channels)
			before, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			blockedPath := filepath.Join(t.TempDir(), "blocked.json")
			opts.DMPolicy = admission.Exclude
			summary, err := Sync(ctx, st, helperProvider(t, "success", blockedPath), opts)
			require.EqualError(t, err, "external provider v1 cannot enforce sync.include_dms=false; use API sync or a supported Slack workspace JSON export")
			require.Equal(t, Summary{}, summary)
			_, err = os.Stat(blockedPath)
			require.ErrorIs(t, err, os.ErrNotExist)
			after, err := st.QueryReadOnly(ctx, "select * from sync_state order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			require.Equal(t, before, after) // Includes updated_at: a rejection must not refresh either cursor.
			lastPath := filepath.Join(t.TempDir(), "last.json")
			opts.DMPolicy = admission.Include
			_, err = Sync(ctx, st, helperProvider(t, "success", lastPath), opts)
			require.NoError(t, err)
			var last request
			require.NoError(t, json.Unmarshal(requireReadFile(t, lastPath), &last))
			require.Equal(t, `{"cursor":"done"}`, last.Checkpoint)
			require.Equal(t, first.Since, last.Since)
			require.Equal(t, first.Channels, last.Channels)
			cursor, err := st.GetSyncState(ctx, "provider:fixture", selected.entityType, selected.entityID)
			require.NoError(t, err)
			require.Equal(t, `{"cursor":"done"}`, cursor)
			untouched, err := st.GetSyncState(ctx, "provider:fixture", other.entityType, other.entityID)
			require.NoError(t, err)
			require.Equal(t, otherSeed, untouched)
		})
	}
}
