package slackapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestUserPrimaryAuthAndHistoryErrors(t *testing.T) {
	for _, mode := range []string{"auth-canceled", "not-in-channel", "missing-scope", "optional-dm-scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var methods []string
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, values url.Values) (any, error) {
				methods = append(methods, r.URL.Path)
				if r.URL.Path == "/auth.test" && mode == "auth-canceled" {
					return nil, context.Canceled
				}
				if r.URL.Path == "/conversations.list" && values.Get("types") == "im,mpim" {
					return map[string]any{"ok": true, "channels": []any{map[string]any{"id": "D123", "is_im": true}}}, nil
				}
				if r.URL.Path == "/conversations.history" {
					if mode == "optional-dm-scope" && values.Get("channel") == "C123" {
						return map[string]any{"ok": true, "messages": []any{}}, nil
					}
					reason := "missing_scope"
					if mode == "not-in-channel" {
						reason = "not_in_channel"
					}
					return map[string]any{"ok": false, "error": reason}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			if mode == "optional-dm-scope" {
				client.WithDMPolicy(admission.Include)
			}
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			if mode == "auth-canceled" {
				require.NoError(t, st.Close())
			}
			err := client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"})
			switch mode {
			case "auth-canceled":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, []string{"/auth.test"}, methods)
			case "missing-scope":
				require.ErrorContains(t, err, "channel C123 history: missing_scope")
				rows, readErr := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='channel_skip' or entity_type='workspace'")
				require.NoError(t, readErr)
				require.Empty(t, rows)
			case "not-in-channel", "optional-dm-scope":
				require.NoError(t, err)
				channel, reason := "C123", "not_in_channel"
				if mode == "optional-dm-scope" {
					channel, reason = "D123", "missing_scope"
				}
				value, readErr := st.GetSyncState(ctx, SourceUser, "channel_skip", channel)
				require.NoError(t, readErr)
				require.Equal(t, reason, value)
			}
			require.NotContains(t, methods, "/conversations.join")
		})
	}
}

func TestUserPrimaryCoverageOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, since, floor, wantOldest string
		full, noUserCoverage           bool
	}{
		{name: "pending", wantOldest: "1709800000.000000"},
		{name: "explicit-since", since: "1709700000.000000", wantOldest: "1709700000.000000"},
		{name: "full", full: true},
		{name: "retention", floor: "1709850000.000000", wantOldest: "1709850000.000000"},
		{name: "bot-coverage-is-not-user-coverage", noUserCoverage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: time.Unix(1700000000, 0)}))
			botCoverage := historyCoverage{Complete: true, Latest: "1710000100.000000"}
			require.NoError(t, saveHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "", botCoverage))
			old := historyCoverage{}
			if !tc.noUserCoverage {
				old = historyCoverage{Complete: true, Latest: "1709900000.000000", Pending: new("1709800000.000000")}
				require.NoError(t, saveHistoryCoverage(ctx, st, SourceUser, "T123", "C123", tc.since, old))
			}
			if tc.floor != "" {
				require.NoError(t, st.SetSyncState(ctx, "retention", "channel_floor", "T123|C123", tc.floor))
				require.NoError(t, st.SetSyncState(ctx, "retention", "channel_seed", "T123|C123", "1"))
			}
			require.NoError(t, st.SetSyncState(ctx, SourceUser, "workspace", "T123", "old-success"))
			before, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-bot' or entity_type='workspace' order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			var historyForms []url.Values
			fail := true
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path == "/conversations.history" {
					historyForms = append(historyForms, form)
					if fail {
						return map[string]any{"ok": false, "error": "synthetic_history_failure"}, nil
					}
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			client.now = func() time.Time { return time.Unix(1710000200, 0).UTC() }
			opts := SyncOptions{WorkspaceID: "T123", Since: tc.since, Full: tc.full}
			require.ErrorContains(t, client.Sync(ctx, st, opts), "synthetic_history_failure")
			pending, err := loadHistoryCoverage(ctx, st, SourceUser, "T123", "C123", tc.since)
			require.NoError(t, err)
			require.Equal(t, old.Complete, pending.Complete)
			require.Equal(t, old.Latest, pending.Latest)
			require.Equal(t, new(tc.wantOldest), pending.Pending)
			after, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-bot' or entity_type='workspace' order by source_name,entity_type,entity_id")
			require.NoError(t, err)
			require.True(t, reflect.DeepEqual(before, after))
			fail = false
			require.NoError(t, client.Sync(ctx, st, opts))
			require.Len(t, historyForms, 2)
			for _, form := range historyForms {
				require.Equal(t, tc.wantOldest, form.Get("oldest"))
				require.Equal(t, "1710000200.000000", form.Get("latest"))
				require.Equal(t, map[bool]string{true: "1", false: "0"}[tc.floor != ""], form.Get("inclusive"))
			}
			complete, err := loadHistoryCoverage(ctx, st, SourceUser, "T123", "C123", tc.since)
			require.NoError(t, err)
			require.Equal(t, historyCoverage{Complete: true, Latest: "1710000200.000000"}, complete)
			botAfter, err := loadHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			require.Equal(t, botCoverage, botAfter)
		})
	}
}

func TestUserPrimaryDoesNotReassignTailClient(t *testing.T) {
	for _, bot := range []string{"", "fixture-bot"} {
		t.Run("bot="+bot, func(t *testing.T) {
			var roles []string
			client := primaryOwnerClient(t, config.Tokens{Bot: bot, User: "fixture-user", App: "fixture-app"}, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path == "/auth.test" || r.URL.Path == "/conversations.list" || r.URL.Path == "/conversations.history" {
					role := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
					if role == "" {
						role = form.Get("token")
					}
					roles = append(roles, r.URL.Path+":"+role)
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			original := client.bot
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			require.NoError(t, client.Sync(context.Background(), st, SyncOptions{WorkspaceID: "T123"}))
			if original == nil {
				require.Nil(t, client.bot)
			} else {
				require.Same(t, original, client.bot)
			}
			roles = nil
			var socketClient *slack.Client
			client.socketModeFn = func(api *slack.Client) socketModeRunner {
				socketClient = api
				return &fakeSocketMode{events: make(chan socketmode.Event)}
			}
			err := client.Tail(context.Background(), st, "T123", 0)
			if bot == "" {
				require.ErrorContains(t, err, "SLACK_BOT_TOKEN is required for tail")
				require.Nil(t, socketClient)
				require.Empty(t, roles)
			} else {
				require.NoError(t, err)
				require.Same(t, original, socketClient)
				require.Equal(t, []string{"/auth.test:fixture-bot"}, roles)
				roles = nil
				require.NoError(t, client.repairWorkspace(context.Background(), st, "T123"))
				require.Equal(t, []string{"/auth.test:fixture-user", "/conversations.list:fixture-bot", "/conversations.history:fixture-bot"}, roles)
			}
		})
	}
}

func TestUserPrimaryPreservesOrdinaryAPIReconciliation(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	for _, role := range []string{"user", "bot"} {
		tokens := config.Tokens{User: "fixture-user"}
		if role == "bot" {
			tokens = config.Tokens{Bot: "fixture-bot"}
		}
		client := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
			if r.URL.Path == "/conversations.history" {
				return map[string]any{"ok": true, "messages": []any{map[string]any{
					"type": "message", "ts": "1710000001.000000", "text": role + " projection <@U" + strings.ToUpper(role) + ">",
					"files": []any{map[string]any{"id": "F" + role, "name": role + ".txt"}},
				}}}, nil
			}
			return primaryOwnerResponse(r.URL.Path), nil
		})
		require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	}
	rows, err := st.QueryReadOnly(ctx, "select text,raw_json,source_name,source_rank from messages")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "bot projection <@UBOT>", rows[0]["text"])
	require.Equal(t, SourceUser, rows[0]["source_name"])
	require.Equal(t, int64(1), rows[0]["source_rank"])
	require.Contains(t, rows[0]["raw_json"], "user projection")
	mentions, err := st.QueryReadOnly(ctx, "select target_id from message_mentions where deleted_at is null")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"target_id": "UBOT"}}, mentions)
	files, err := st.QueryReadOnly(ctx, "select file_id from message_files where deleted_at is null")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"file_id": "Fbot"}}, files)
	fts, err := st.Search(ctx, "T123", "projection", 10)
	require.NoError(t, err)
	require.Len(t, fts, 1)
	require.Contains(t, fts[0].NormalizedText, "bot projection")
	for _, table := range []string{"message_events", "message_event_heads"} {
		rows, err := st.QueryReadOnly(ctx, "select source_name from "+table+" order by source_name")
		require.NoError(t, err)
		require.Equal(t, []map[string]any{{"source_name": SourceBot}, {"source_name": SourceUser}}, rows)
	}
}

func TestUserOnlyDoctorAuthDiagnostics(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid", true: "canceled"}[canceled], func(t *testing.T) {
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user", App: "fixture-app"}, func(r *http.Request, _ url.Values) (any, error) {
				require.Equal(t, "/auth.test", r.URL.Path)
				if canceled {
					return nil, context.Canceled
				}
				return map[string]any{"ok": false, "error": "invalid_auth"}, nil
			})
			diag, err := client.Doctor(context.Background())
			require.NoError(t, err)
			require.True(t, diag.UserConfigured)
			require.False(t, diag.BotConfigured)
			require.False(t, diag.UserAuthAvailable)
			require.False(t, diag.AppTailAvailable)
			require.Equal(t, "partial", diag.ThreadCoverage)
			require.NotEmpty(t, diag.UserAuthError)
		})
	}
}

type primaryOwnerRoundTrip func(*http.Request) (*http.Response, error)

func (fn primaryOwnerRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func primaryOwnerClient(t *testing.T, tokens config.Tokens, respond func(*http.Request, url.Values) (any, error)) *Client {
	t.Helper()
	transport := primaryOwnerRoundTrip(func(r *http.Request) (*http.Response, error) {
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		payload, err := respond(r, r.Form)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
	})
	return NewWithOptions(tokens, "https://fixture.invalid/", &http.Client{Transport: transport}).WithDMPolicy(admission.Exclude)
}

func primaryOwnerResponse(method string) any {
	switch method {
	case "/auth.test":
		return map[string]any{"ok": true, "team_id": "T123", "team": "Fixture"}
	case "/conversations.list":
		return map[string]any{"ok": true, "channels": []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true}}}
	case "/conversations.history":
		return map[string]any{"ok": true, "messages": []any{}}
	case "/users.list":
		return map[string]any{"ok": true, "members": []any{}}
	default:
		return map[string]any{"ok": false, "error": "unexpected_fixture_method"}
	}
}
