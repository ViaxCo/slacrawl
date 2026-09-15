package slackmcp

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/store"
)

func TestExplicitSinceLeavesUnreturnedRetainedRootsAlone(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full=%t", full), func(t *testing.T) {
			ctx := context.Background()
			st := admissionStore(t)
			now := time.Unix(1700000000, 0)
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.UpsertMessage(ctx, store.Message{
				ChannelID: "C123", WorkspaceID: "T123", TS: "1700000000.000001", ReplyCount: 1,
				Text: "old root", NormalizedText: "old root", RawJSON: "{}",
				SourceName: SourceName, SourceRank: SourceRank, UpdatedAt: now,
			}, nil))
			_, err := st.PrepareThreadWork(ctx, SourceName, "T123", "C123")
			require.NoError(t, err)
			before, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			require.Len(t, before, 1)
			var threadCalls atomic.Int32
			server := admissionGateway(t, true, func(name string, _ map[string]any) map[string]any {
				switch name {
				case "slack_get_channel_history":
					return map[string]any{"ok": true, "messages": []any{
						map[string]any{"ts": "1710000000.000001", "text": "recent message"},
					}}
				case "slack_get_thread_replies":
					threadCalls.Add(1)
					return map[string]any{"ok": true, "messages": []any{}}
				default:
					return map[string]any{"ok": true, "channels": []any{}, "members": []any{}}
				}
			})
			defer server.Close()
			t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
			t.Setenv("TEST_MCP_ACCOUNT", "")
			cfg := testMCPConfig(server.URL)
			cfg.ConnectorID = ""
			_, err = Sync(ctx, st, Options{WorkspaceID: "T123", Channels: []string{"C123"}, Since: "1709000000.000000", Full: full, Config: cfg})
			require.NoError(t, err)
			require.Zero(t, threadCalls.Load(), "an explicit history slice must not fetch an unreturned older root")
			after, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
