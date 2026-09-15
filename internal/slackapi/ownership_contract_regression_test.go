package slackapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverlappingSyncPreservesNewerHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	olderStore, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, olderStore.Close()) })
	newerStore, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, newerStore.Close()) })
	tokens := config.Tokens{User: "fixture-user"}
	newer := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
		if r.URL.Path == "/conversations.history" {
			return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "1710000000.000000", "text": "newer content"}}}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	newer.now = func() time.Time { return time.Unix(1710000300, 0) }
	older := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
		if r.URL.Path == "/conversations.history" {
			require.NoError(t, newer.Sync(ctx, newerStore, SyncOptions{WorkspaceID: "T123"}))
			return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "1710000000.000000", "text": "stale content"}}}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	older.now = func() time.Time { return time.Unix(1710000200, 0) }
	assert.Error(t, older.Sync(ctx, olderStore, SyncOptions{WorkspaceID: "T123"}))
	rows, err := olderStore.QueryReadOnly(ctx, "select text from messages")
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"text": "newer content"}}, rows)
}

func TestSyncPublicationCannotHideNewerFailedHistory(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	tokens := config.Tokens{User: "fixture-user"}
	failure := errors.New("synthetic history failure")
	newer := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
		if r.URL.Path == "/conversations.history" {
			return nil, failure
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	newer.now = func() time.Time { return time.Unix(1710000300, 0) }
	older := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
		if r.URL.Path == "/users.list" {
			require.ErrorIs(t, newer.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}), failure)
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	older.now = func() time.Time { return time.Unix(1710000200, 0) }
	require.NoError(t, older.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	status, err := st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "partial", status.ThreadState)
}

func TestHistoryCollisionRetriesUnfinishedInterval(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.UpsertMessage(ctx, store.Message{WorkspaceID: "TOTHER", ChannelID: "C123", TS: "1710000000.000000", Text: "foreign", NormalizedText: "foreign", SourceName: SourceUser, SourceRank: 1, RawJSON: "{}", UpdatedAt: time.Unix(1710000000, 0)}, nil))
	var oldest []string
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		if r.URL.Path == "/conversations.history" {
			oldest = append(oldest, form.Get("oldest"))
			if len(oldest) == 1 {
				return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "1710000000.000000", "text": "collision"}}}, nil
			}
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	client.now = func() time.Time { return time.Unix(1710000200, 0) }
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	require.Equal(t, []string{"", ""}, oldest, "a rejected row must not complete the requested interval")
}
