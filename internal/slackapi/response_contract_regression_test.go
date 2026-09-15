package slackapi

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
)

func TestSyncRejectsUncertifiedNativeResponses(t *testing.T) {
	collections := map[string]string{
		"/conversations.list": "channels", "/users.list": "members",
		"/conversations.history": "messages", "/conversations.replies": "messages",
	}
	for _, method := range []string{"/auth.test", "/conversations.list", "/users.list", "/conversations.history", "/conversations.replies"} {
		faults := []string{"missing-ok", "null-ok", "false-ok"}
		if collections[method] != "" {
			faults = append(faults, "missing-collection", "null-collection")
		}
		for _, fault := range faults {
			t.Run(strings.TrimPrefix(method, "/")+"/"+fault, func(t *testing.T) {
				ctx := context.Background()
				st := mustStore(t)
				defer func() { require.NoError(t, st.Close()) }()
				require.NoError(t, st.SetSyncState(ctx, SourceBot, "workspace", "T123", "previous success"))
				client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
					payload := responseContractPayload(r.URL.Path, method == "/conversations.replies")
					if r.URL.Path == method {
						switch fault {
						case "missing-ok":
							delete(payload, "ok")
						case "null-ok":
							payload["ok"] = nil
						case "false-ok":
							payload["ok"] = false
						case "missing-collection":
							delete(payload, collections[method])
						case "null-collection":
							payload[collections[method]] = nil
						}
					}
					return payload, nil
				})
				require.Error(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
				previous, err := st.GetSyncState(ctx, SourceBot, "workspace", "T123")
				require.NoError(t, err)
				require.Equal(t, "previous success", previous)
				rows, err := st.QueryReadOnly(ctx, "select * from messages where ts='1710000002.000000'")
				require.NoError(t, err)
				require.Empty(t, rows, "an uncertified reply must not enter the archive")
			})
		}
	}
}

func TestAuxiliaryNativeResponsesRequireSuccess(t *testing.T) {
	for _, method := range []string{"/conversations.info", "/conversations.join"} {
		for _, fault := range []string{"missing", "null", "false"} {
			t.Run(strings.TrimPrefix(method, "/")+"/"+fault, func(t *testing.T) {
				client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot"}, func(r *http.Request, _ url.Values) (any, error) {
					require.Equal(t, method, r.URL.Path)
					payload := map[string]any{"channel": map[string]any{"id": "C123", "is_channel": true}}
					if fault == "null" {
						payload["ok"] = nil
					} else if fault == "false" {
						payload["ok"] = false
					}
					return payload, nil
				})
				if method == "/conversations.info" {
					allowed, err := client.admitTailConversation(context.Background(), "T123", "C123", "")
					require.Error(t, err)
					require.False(t, allowed)
				} else {
					require.Error(t, client.joinConversation(context.Background(), "C123"))
				}
			})
		}
	}
}

func TestSyncRejectsUnboundAuthenticatedWorkspace(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
		payload := responseContractPayload(r.URL.Path, false)
		if r.URL.Path == "/auth.test" {
			payload["team_id"] = ""
		}
		return payload, nil
	})
	require.Error(t, client.Sync(ctx, st, SyncOptions{}))
	rows, err := st.QueryReadOnly(ctx, "select * from workspaces")
	require.NoError(t, err)
	require.Empty(t, rows)
}

func responseContractPayload(method string, withThread bool) map[string]any {
	if method == "/conversations.history" && withThread {
		return map[string]any{"ok": true, "messages": []any{
			map[string]any{"ts": "1710000001.000000", "reply_count": 1, "text": "valid root"},
		}}
	}
	if method == "/conversations.replies" {
		return map[string]any{"ok": true, "messages": []any{
			map[string]any{"ts": "1710000001.000000", "text": "valid root"},
			map[string]any{"ts": "1710000002.000000", "thread_ts": "1710000001.000000", "text": "uncertified reply"},
		}}
	}
	return primaryOwnerResponse(method).(map[string]any)
}
