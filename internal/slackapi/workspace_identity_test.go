package slackapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestSyncRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	for _, workspaceID := range []string{"", "T123"} {
		t.Run("requested="+workspaceID, func(t *testing.T) {
			server := newMismatchedUserSlackServer(t)
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			err := client.Sync(context.Background(), st, SyncOptions{WorkspaceID: workspaceID})
			require.ErrorContains(t, err, "user token: authenticated workspace T999 does not match requested workspace T123")
			require.Equal(t, 2, server.calls("auth.test"))
			assertNoWorkspaceDataRequests(t, server)
			assertEmptyWorkspaceArchive(t, st)
		})
	}
}

func TestRepairRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	server := newMismatchedUserSlackServer(t)
	defer server.Close()
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	err := client.repairWorkspace(context.Background(), st, "T123")
	require.ErrorContains(t, err, "user token: authenticated workspace T999 does not match requested workspace T123")
	require.Equal(t, 1, server.calls("auth.test"))
	assertNoWorkspaceDataRequests(t, server)
	assertEmptyWorkspaceArchive(t, st)
}

func TestDoctorRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	server := newMismatchedUserSlackServer(t)
	defer server.Close()
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client())
	diag, err := client.Doctor(context.Background())
	require.NoError(t, err)
	require.True(t, diag.UserConfigured)
	require.Equal(t, "T123", diag.BotAuthTeamID)
	require.False(t, diag.UserAuthAvailable)
	require.Equal(t, "partial", diag.ThreadCoverage)
	require.False(t, diag.DMsIncluded)
	require.Empty(t, diag.DMsMissingScope)
	require.Contains(t, diag.UserAuthError, "authenticated workspace T999 does not match requested workspace T123")
	require.Equal(t, 2, server.calls("auth.test"))
	assertNoWorkspaceDataRequests(t, server)
}

func assertNoWorkspaceDataRequests(t *testing.T, server *mockSlackServer) {
	t.Helper()
	for _, endpoint := range []string{"conversations.list", "conversations.history", "conversations.replies", "users.list"} {
		require.Zero(t, server.calls(endpoint), endpoint)
	}
}

func assertEmptyWorkspaceArchive(t *testing.T, st *store.Store) {
	t.Helper()
	for _, table := range []string{
		"workspaces", "channels", "users", "messages", "message_files", "message_events",
		"message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state",
	} {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		require.Empty(t, rows, table)
	}
}

func newMismatchedUserSlackServer(t *testing.T) *mockSlackServer {
	t.Helper()
	mock := &mockSlackServer{counts: map[string]int{}}
	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		mock.counts[r.URL.Path]++
		mock.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			teamID := "T123"
			if mustFormValues(r).Get("token") == "xoxp-test" || strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == "xoxp-test" {
				teamID = "T999"
			}
			// The same Enterprise Grid organization does not make teams interchangeable.
			_, _ = fmt.Fprintf(w, `{"ok":true,"team_id":%q,"enterprise_id":"E123"}`, teamID)
		case "/conversations.list":
			_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C123","name":"general","is_channel":true}],"response_metadata":{"next_cursor":""}}`))
		case "/conversations.history":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","text":"root","ts":"1710000000.000100","reply_count":1}],"response_metadata":{"next_cursor":""}}`))
		case "/conversations.replies":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","text":"foreign-workspace-reply","ts":"1710000001.000100","thread_ts":"1710000000.000100"}],"response_metadata":{"next_cursor":""}}`))
		case "/users.list":
			_, _ = w.Write([]byte(`{"ok":true,"members":[],"response_metadata":{"next_cursor":""}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	return mock
}

var unboundWorkspaceAuth = []struct{ name, payload string }{
	{"missing", `{"ok":true,"team":"unbound-auth-canary","user_id":"U123","enterprise_id":"E123"}`},
	{"null", `{"ok":true,"team_id":null,"team":"unbound-auth-canary","user_id":"U123","enterprise_id":"E123"}`},
	{"empty", `{"ok":true,"team_id":"","team":"unbound-auth-canary","user_id":"U123","enterprise_id":"E123"}`},
	{"whitespace", `{"ok":true,"team_id":" \t\n ","team":"unbound-auth-canary","user_id":"U123","enterprise_id":"E123"}`},
}

const unboundWorkspaceError = "auth.test did not identify a workspace; use a workspace-scoped bot or user token"

func TestSyncRequiresBoundPrimaryWorkspace(t *testing.T) {
	for _, response := range unboundWorkspaceAuth {
		for _, primary := range []string{"bot", "user"} {
			for _, requested := range []string{"", "T123"} {
				t.Run(response.name+"/"+primary+"/requested="+requested, func(t *testing.T) {
					st := mustStore(t)
					defer func() { require.NoError(t, st.Close()) }()
					before := seedWorkspaceIdentityArchive(t, st)
					tokens := config.Tokens{User: "fixture-user"}
					if primary == "bot" {
						tokens.Bot = "fixture-bot"
					}
					var calls []string
					client := primaryOwnerClient(t, tokens, func(r *http.Request, form url.Values) (any, error) {
						calls = append(calls, r.URL.Path+":"+form.Get("token"))
						require.Equal(t, "/auth.test", r.URL.Path)
						return json.RawMessage(response.payload), nil
					})
					err := client.Sync(context.Background(), st, SyncOptions{WorkspaceID: requested})
					require.EqualError(t, err, unboundWorkspaceError)
					require.Equal(t, []string{"/auth.test:fixture-" + primary}, calls, "configured bot failure never falls back to user auth")
					require.Equal(t, before, workspaceIdentityRows(t, st))
					assertAdmissionCanariesAbsent(t, st, err.Error(), "unbound-auth-canary")
				})
			}
		}
	}
}

func TestOptionalUserRequiresBoundWorkspace(t *testing.T) {
	for _, response := range unboundWorkspaceAuth {
		for _, repair := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/repair=%t", response.name, repair), func(t *testing.T) {
				st := mustStore(t)
				defer func() { require.NoError(t, st.Close()) }()
				before := seedWorkspaceIdentityArchive(t, st)
				var calls []string
				client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
					calls = append(calls, r.URL.Path+":"+form.Get("token"))
					require.Equal(t, "/auth.test", r.URL.Path)
					if form.Get("token") == "fixture-bot" {
						return primaryOwnerResponse(r.URL.Path), nil
					}
					require.Equal(t, "fixture-user", form.Get("token"))
					return json.RawMessage(response.payload), nil
				})
				var err error
				if repair {
					err = client.repairWorkspace(context.Background(), st, "T123")
					require.Equal(t, []string{"/auth.test:fixture-user"}, calls)
				} else {
					err = client.Sync(context.Background(), st, SyncOptions{WorkspaceID: "T123"})
					require.Equal(t, []string{"/auth.test:fixture-bot", "/auth.test:fixture-user"}, calls)
				}
				require.EqualError(t, err, "user token: "+unboundWorkspaceError)
				require.Equal(t, before, workspaceIdentityRows(t, st), "successful but unbound auth is not optional-auth fallback")
				assertAdmissionCanariesAbsent(t, st, err.Error(), "unbound-auth-canary")
			})
		}
	}
}

func TestTailRequiresBoundWorkspaceBeforeRunner(t *testing.T) {
	for _, response := range unboundWorkspaceAuth {
		t.Run(response.name, func(t *testing.T) {
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			before := seedWorkspaceIdentityArchive(t, st)
			calls := 0
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", App: "fixture-app"}, func(r *http.Request, form url.Values) (any, error) {
				calls++
				require.Equal(t, "/auth.test", r.URL.Path)
				require.Equal(t, "fixture-bot", form.Get("token"))
				return json.RawMessage(response.payload), nil
			})
			client.socketModeFn = func(*slack.Client) socketModeRunner {
				t.Fatal("unbound workspace must not construct Socket Mode")
				return nil
			}
			err := client.Tail(context.Background(), st, "T123", 0)
			require.EqualError(t, err, unboundWorkspaceError)
			require.Equal(t, 1, calls)
			require.Equal(t, before, workspaceIdentityRows(t, st))
			assertAdmissionCanariesAbsent(t, st, err.Error(), "unbound-auth-canary")
		})
	}
}

func TestDoctorRequiresBoundWorkspace(t *testing.T) {
	for _, response := range unboundWorkspaceAuth {
		for _, role := range []string{"bot", "optional-user", "user-only"} {
			t.Run(response.name+"/"+role, func(t *testing.T) {
				tokens := config.Tokens{Bot: "fixture-bot", User: "fixture-user", App: "fixture-app"}
				if role == "user-only" {
					tokens.Bot = ""
				}
				var calls []string
				client := primaryOwnerClient(t, tokens, func(r *http.Request, form url.Values) (any, error) {
					calls = append(calls, r.URL.Path+":"+form.Get("token"))
					require.Equal(t, "/auth.test", r.URL.Path, "unbound auth must not reach DM probes")
					if role == "optional-user" && form.Get("token") == "fixture-bot" {
						return primaryOwnerResponse(r.URL.Path), nil
					}
					return json.RawMessage(response.payload), nil
				}).WithDMPolicy(admission.Include)
				diag, err := client.Doctor(context.Background())
				require.Equal(t, "partial", diag.ThreadCoverage)
				require.False(t, diag.UserAuthAvailable)
				require.False(t, diag.DMsIncluded)
				require.Empty(t, diag.DMsMissingScope)
				if role == "bot" {
					require.EqualError(t, err, unboundWorkspaceError)
					require.Equal(t, []string{"/auth.test:fixture-bot"}, calls)
					require.Empty(t, diag.BotAuthTeamID)
					require.Empty(t, diag.BotAuthTeam)
					require.False(t, diag.AppTailAvailable)
				} else {
					require.NoError(t, err)
					require.Equal(t, unboundWorkspaceError, diag.UserAuthError)
					if role == "optional-user" {
						require.Equal(t, []string{"/auth.test:fixture-bot", "/auth.test:fixture-user"}, calls)
						require.Equal(t, "T123", diag.BotAuthTeamID)
						require.True(t, diag.AppTailAvailable)
					} else {
						require.Equal(t, []string{"/auth.test:fixture-user"}, calls)
						require.Empty(t, diag.BotAuthTeamID)
						require.False(t, diag.AppTailAvailable)
					}
				}
				encoded, marshalErr := json.Marshal(diag)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(encoded), "unbound-auth-canary")
			})
		}
	}
}

func TestBoundWorkspaceIdentityControls(t *testing.T) {
	for _, primary := range []string{"bot", "user"} {
		for _, identity := range []string{"valid", "trimmed", "enterprise"} {
			t.Run(primary+"/"+identity, func(t *testing.T) {
				ctx := context.Background()
				tokens := config.Tokens{User: "fixture-user"}
				if primary == "bot" {
					tokens = config.Tokens{Bot: "fixture-bot", App: "fixture-app"}
				}
				teamID, requested, enterpriseID := "T123", "", ""
				if identity == "trimmed" {
					teamID, requested = " \tT123\n", " T123 "
				}
				if identity == "enterprise" {
					enterpriseID, requested = "E123", "T123"
				}
				var catalogs []string
				client := primaryOwnerClient(t, tokens, func(r *http.Request, form url.Values) (any, error) {
					require.Equal(t, "fixture-"+primary, form.Get("token"))
					switch r.URL.Path {
					case "/auth.test":
						return map[string]any{"ok": true, "team_id": teamID, "enterprise_id": enterpriseID, "team": "fixture"}, nil
					case "/conversations.list":
						require.Equal(t, "T123", form.Get("team_id"))
						catalogs = append(catalogs, form.Get("types"))
						return json.RawMessage(`{"ok":true,"channels":[]}`), nil
					case "/users.list":
						require.Empty(t, form.Get("team_id"), "workspace tokens retain the existing users.list form")
						return json.RawMessage(`{"ok":true,"members":[]}`), nil
					default:
						t.Fatalf("unexpected request %s", r.URL.Path)
						return nil, nil
					}
				}).WithDMPolicy(admission.Include)
				st := mustStore(t)
				defer func() { require.NoError(t, st.Close()) }()
				require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: requested}))
				require.Equal(t, []map[string]any{{"id": "T123", "enterprise_id": enterpriseID}}, repairKeyRows(t, st, "select id,enterprise_id from workspaces"))
				catalogs = nil
				diag, err := client.Doctor(ctx)
				require.NoError(t, err)
				if primary == "bot" {
					require.Equal(t, "T123", diag.BotAuthTeamID)
					require.True(t, diag.AppTailAvailable)
					require.Equal(t, "partial", diag.ThreadCoverage)
					require.Empty(t, catalogs)
				} else {
					require.True(t, diag.UserAuthAvailable)
					require.True(t, diag.DMsIncluded)
					require.Empty(t, diag.DMsMissingScope)
					require.Equal(t, "full", diag.ThreadCoverage)
					require.Equal(t, []string{"im,mpim"}, catalogs, "the actual Doctor probe uses the canonical workspace ID")
				}
			})
		}
	}
}

func seedWorkspaceIdentityArchive(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	ctx := context.Background()
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TKEEP", Name: "retained", RawJSON: `{}`, UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "CKEEP", WorkspaceID: "TKEEP", Kind: "public_channel", RawJSON: `{}`, UpdatedAt: now}))
	require.NoError(t, st.UpsertUser(ctx, store.User{ID: "UKEEP", WorkspaceID: "TKEEP", Name: "retained", RawJSON: `{}`, UpdatedAt: now}))
	require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "CKEEP", WorkspaceID: "TKEEP", TS: "1710000000.000000", UserID: "UKEEP", Text: "retained", NormalizedText: "retained", RawJSON: `{}`, SourceName: SourceBot, SourceRank: 2, UpdatedAt: now, Files: []store.MessageFile{{FileID: "FKEEP", Name: "retained.txt"}}}, []store.Mention{{Type: "user", TargetID: "UKEEP", DisplayText: "retained"}}))
	require.NoError(t, st.SetSyncState(ctx, SourceBot, "workspace", "T123", "2020-01-01T00:00:00Z"))
	_, err := st.DB().ExecContext(ctx, "insert into embedding_jobs(channel_id,ts,state,created_at) values(?,?,?,?)", "CKEEP", "1710000000.000000", "pending", now.Format(time.RFC3339))
	require.NoError(t, err)
	rows := workspaceIdentityRows(t, st)
	for table, data := range rows {
		require.NotEmpty(t, data, "seed every admission table: %s", table)
	}
	return rows
}

func workspaceIdentityRows(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	rows := map[string][]map[string]any{}
	for _, table := range admissionTables {
		rows[table] = repairKeyRows(t, st, "select * from "+table)
	}
	return rows
}
