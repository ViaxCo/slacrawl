package slackapi

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestUnavailableRetainedRootDoesNotBlockHealthyWork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	const missing = "1710000001.000000"
	roots := []struct{ channel, ts string }{
		{"C123", missing},
		{"C123", "1710000002.000000"},
		{"COTHER", "1710000003.000000"},
	}
	for _, root := range roots {
		require.NoError(t, st.UpsertChannel(ctx, store.Channel{
			ID: root.channel, WorkspaceID: "T123", Name: root.channel,
			Kind: "public_channel", RawJSON: "{}", UpdatedAt: time.Unix(1710000000, 0),
		}))
		require.NoError(t, st.UpsertMessage(ctx, store.Message{
			ChannelID: root.channel, WorkspaceID: "T123", TS: root.ts, ThreadTS: root.ts,
			ReplyCount: 1, Text: "retained root", NormalizedText: "retained root",
			SourceName: SourceBot, SourceRank: 2, RawJSON: "{}", UpdatedAt: time.Unix(1710000000, 0),
		}, nil))
	}
	for _, channel := range []string{"C123", "COTHER"} {
		require.NoError(t, seedAPIHistory(ctx, st, SourceBot, "T123", channel, "", store.APIHistoryState{
			Complete: true, Latest: "1710100000.000000",
		}))
	}
	for attempt := 0; attempt < 2; attempt++ {
		var replies, histories []string
		client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
			switch r.URL.Path {
			case "/conversations.list":
				return map[string]any{"ok": true, "channels": []any{
					map[string]any{"id": "C123", "name": "fixture", "is_channel": true},
					map[string]any{"id": "COTHER", "name": "other", "is_channel": true},
				}}, nil
			case "/conversations.history":
				require.NotEmpty(t, form.Get("oldest"))
				histories = append(histories, form.Get("channel"))
			case "/conversations.replies":
				replies = append(replies, form.Get("channel")+"|"+form.Get("ts"))
				if form.Get("ts") == missing {
					return map[string]any{"ok": false, "error": "thread_not_found"}, nil
				}
				return map[string]any{"ok": true, "messages": []any{}}, nil
			}
			return primaryOwnerResponse(r.URL.Path), nil
		})
		require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Concurrency: 1}))
		require.Equal(t, []string{"C123|" + missing, "C123|1710000002.000000", "COTHER|1710000003.000000"}, replies)
		require.Equal(t, []string{"C123", "COTHER"}, histories)
		pending, err := st.QueryReadOnly(ctx, "select entity_id from sync_state where source_name='api-user' and entity_type='thread_pending_v1'")
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Contains(t, pending[0]["entity_id"], missing)
		reason, err := st.GetSyncState(ctx, SourceUser, "thread_skip", "T123|C123|"+missing)
		require.NoError(t, err)
		require.Equal(t, "thread_not_found", reason)
		archive, err := st.Status(ctx)
		require.NoError(t, err)
		require.Equal(t, "partial", archive.ThreadState)
		require.Equal(t, 3, archive.Messages)
		if attempt == 0 {
			require.NoError(t, st.Close())
			st, err = store.Open(path)
			require.NoError(t, err)
		}
	}
}

func TestUnavailableScopedThreadStillFails(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
		switch r.URL.Path {
		case "/conversations.history":
			return map[string]any{"ok": true, "messages": []any{
				map[string]any{"ts": "1710000001.000000", "reply_count": 1, "text": "scoped root"},
			}}, nil
		case "/conversations.replies":
			return map[string]any{"ok": false, "error": "thread_not_found"}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	err := client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Since: "1709900000.000000"})
	require.ErrorContains(t, err, "thread_not_found")
	pending, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestUnavailableRetainedRootKeepsFullSyncPartialAfterConcurrentCompletion(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	const root = "1710000001.000000"
	retainedOwnerSeed(t, st, root)
	require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy", "retained warning"))
	completed := false
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		switch r.URL.Path {
		case "/conversations.list":
			channels := []any{}
			if form.Get("types") != "im,mpim" {
				channels = []any{
					map[string]any{"id": "C123", "is_channel": true},
					map[string]any{"id": "COTHER", "is_channel": true},
				}
			}
			return map[string]any{"ok": true, "channels": channels}, nil
		case "/conversations.replies":
			return map[string]any{"ok": false, "error": "thread_not_found"}, nil
		case "/conversations.history":
			if form.Get("channel") == "COTHER" {
				// Another worker completes the root after this scan observed its omission.
				work, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
				require.NoError(t, err)
				require.Len(t, work, 1)
				completed, err = st.CompleteThreadWork(ctx, work[0], "T123|C123|"+root, nil)
				require.NoError(t, err)
			}
		}
		return primaryOwnerResponse(r.URL.Path), nil
	}).WithDMPolicy(admission.Include)
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true, Concurrency: 1}))
	require.True(t, completed)
	value, err := st.GetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy")
	require.NoError(t, err)
	require.Equal(t, "retained warning", value)
	archive, err := st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "partial", archive.ThreadState)
	work, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
	require.NoError(t, err)
	require.Empty(t, work)
}
