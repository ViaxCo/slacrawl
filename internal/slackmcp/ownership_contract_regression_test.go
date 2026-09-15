package slackmcp

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSupersededMCPHistoryDoesNotWriteStalePage(t *testing.T) {
	ctx := context.Background()
	st, other := overlapMCPStores(t)
	gateway := newCoverageGateway(t, false, func(call coverageCall, payload map[string]any) int {
		work, selected, err := other.BeginMCPHistory(ctx, store.MCPHistoryScope{WorkspaceID: "TLOCAL", ChannelID: call.Channel, Adapter: "reference"}, store.MCPHistoryOptions{})
		require.NoError(t, err)
		require.True(t, selected)
		require.NotEmpty(t, work.Revision)
		payload["messages"] = []map[string]any{{"ts": "1710000000.000001", "text": "stale history"}}
		return http.StatusOK
	})
	opts := coverageOptions(t, gateway, admission.Default)
	opts.Channels = []string{"CFIRST"}
	_, err := Sync(ctx, st, opts)
	require.Error(t, err)
	rows, err := st.QueryReadOnly(ctx, "select text from messages")
	require.NoError(t, err)
	require.Empty(t, rows, "supersession must reject the page before persistence")
}

func TestSupersededScopedMCPReplyPreservesNewerRoot(t *testing.T) {
	ctx := context.Background()
	st, other := overlapMCPStores(t)
	const root = "1710000000.000001"
	gateway := newCoverageGateway(t, true, func(call coverageCall, payload map[string]any) int {
		if call.Thread == "" {
			payload["messages"] = []map[string]any{{"ts": root, "text": "history root", "reply_count": 1}}
			return http.StatusOK
		}
		work, err := other.PrepareThreadWork(ctx, SourceName, "TLOCAL", call.Channel, nil)
		require.NoError(t, err)
		require.Len(t, work, 1)
		require.NoError(t, other.UpsertMessage(ctx, store.Message{WorkspaceID: "TLOCAL", ChannelID: call.Channel, TS: root, ReplyCount: 1, Text: "newer root", NormalizedText: "newer root", SourceName: SourceName, SourceRank: SourceRank, RawJSON: "{}", UpdatedAt: time.Now().UTC()}, nil))
		payload["messages"] = []map[string]any{{"ts": root, "text": "stale reply parent", "reply_count": 1}, {"ts": "1710000000.000002", "thread_ts": root, "text": "stale child"}}
		return http.StatusOK
	})
	opts := coverageOptions(t, gateway, admission.Default)
	opts.Channels = []string{"CFIRST"}
	opts.Since = "1709000000.000000"
	_, err := Sync(ctx, st, opts)
	require.NoError(t, err)
	rows, err := st.QueryReadOnly(ctx, "select text from messages order by ts")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"text": "newer root"}}, rows)
}

func overlapMCPStores(t *testing.T) (*store.Store, *store.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	other, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	return st, other
}
