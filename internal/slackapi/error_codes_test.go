package slackapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
)

func requireNativeErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var native slack.SlackErrorResponse
	require.ErrorAs(t, err, &native)
	require.Equal(t, code, native.Err)
}

func TestNativeErrorCodesRenderSafely(t *testing.T) {
	for _, method := range []string{"auth.test", "conversations.info", "conversations.join", "conversations.list", "users.list", "conversations.history", "conversations.replies"} {
		for _, tc := range []struct {
			name, code string
			readable   bool
		}{
			{"scope", "missing_scope", true},
			{"membership", "not_in_channel", true},
			{"channel", "channel_not_found", true},
			{"auth", "invalid_auth", true},
			{"unauthenticated", "not_authed", true},
			{"inactive", "account_inactive", true},
			{"expired", "token_expired", true},
			{"revoked", "token_revoked", true},
			{"archived", "is_archived", true},
			{"arbitrary", "native-error-canary", false},
			{"unknown-code", "unexpected_error_code", false},
			{"url", "https://fixture.invalid/native-error-canary", false},
			{"suffix", "missing_scope: native-error-canary", false},
			{"prefix", "native-error-canary missing_scope", false},
			{"whitespace", " missing_scope ", false},
			{"case", "MISSING_SCOPE", false},
			{"newline", "missing_scope\nnative-error-canary", false},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				calls := 0
				client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
					calls++
					require.Equal(t, "/"+method, r.URL.Path)
					return map[string]any{"ok": false, "error": tc.code, "errors": []string{"native-error-detail-canary"},
						"team_id": "T123", "channel": map[string]any{"id": "C123"}, "channels": []any{}, "members": []any{}, "messages": []any{},
						"response_metadata": map[string]any{"next_cursor": "native-cursor-canary"}}, nil
				})
				err := nativeResponseCall(t, context.Background(), client, method)
				want := "slack " + method + " API response failed"
				if tc.readable {
					want = tc.code
				}
				require.EqualError(t, err, want)
				require.NotContains(t, fmt.Sprintf("%+v", err), "canary")
				require.NotContains(t, fmt.Errorf("caller: %w", err).Error(), "canary")
				var native slack.SlackErrorResponse
				require.ErrorAs(t, err, &native)
				require.Equal(t, tc.code, native.Err)
				require.Equal(t, []slack.SlackResponseErrors{{Message: new("native-error-detail-canary")}}, native.Errors)
				require.Equal(t, 1, calls, "native errors do not retry or traverse their cursor")
			})
		}
	}
}

func TestNativeSuccessfulResponsesIgnoreErrorStrings(t *testing.T) {
	for _, method := range []string{"auth.test", "conversations.info", "conversations.join", "conversations.list", "users.list", "conversations.history", "conversations.replies"} {
		t.Run(method, func(t *testing.T) {
			calls := 0
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
				calls++
				require.Equal(t, "/"+method, r.URL.Path)
				return map[string]any{"ok": true, "error": "native-error-canary", "team_id": "T123", "channel": map[string]any{"id": "C123"}, "channels": []any{}, "members": []any{}, "messages": []any{}}, nil
			})
			require.NoError(t, nativeResponseCall(t, context.Background(), client, method), "preserve the SDK's explicit-success short circuit")
			require.Equal(t, 1, calls)
		})
	}
}

func TestDoctorOmitsNativeErrorStrings(t *testing.T) {
	for _, primary := range []bool{false, true} {
		t.Run(fmt.Sprintf("primary=%t", primary), func(t *testing.T) {
			var calls []string
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				require.Equal(t, "/auth.test", r.URL.Path, "unavailable auth must not reach a DM probe")
				calls = append(calls, form.Get("token"))
				if !primary && form.Get("token") == "fixture-bot" {
					return primaryOwnerResponse(r.URL.Path), nil
				}
				return map[string]any{"ok": false, "error": "native-auth-canary"}, nil
			}).WithDMPolicy(admission.Include)
			diag, err := client.Doctor(context.Background())
			if primary {
				require.EqualError(t, err, "slack auth.test API response failed")
				requireNativeErrorCode(t, err, "native-auth-canary")
				require.Equal(t, []string{"fixture-bot"}, calls, "failed bot auth does not fall back to the user")
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"fixture-bot", "fixture-user"}, calls)
				require.Equal(t, "T123", diag.BotAuthTeamID)
				require.True(t, diag.UserConfigured)
				require.False(t, diag.UserAuthAvailable)
				require.False(t, diag.DMsIncluded)
				require.Empty(t, diag.DMsMissingScope)
				require.Equal(t, "partial", diag.ThreadCoverage)
				require.Equal(t, "slack auth.test API response failed", diag.UserAuthError)
			}
			encoded, encodeErr := json.Marshal(diag)
			require.NoError(t, encodeErr)
			require.NotContains(t, string(encoded), "native-auth-canary")
		})
	}
}
