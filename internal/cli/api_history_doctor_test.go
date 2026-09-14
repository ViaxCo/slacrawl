package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestDoctorRetainedAPIHistory(t *testing.T) {
	for _, tc := range []struct {
		name, source, history, wantGlobal, wantTop, reason, detail       string
		thread, named, globalPartial, namedPartial, missingDB, wantError bool
	}{
		{name: "bot-history", source: "api-bot", history: "pending", wantGlobal: "partial", wantTop: "partial", reason: "retained_api_history_work", detail: "partial: incomplete API history"},
		{name: "user-history", source: "api-user", history: "pending", wantGlobal: "partial", wantTop: "partial", reason: "retained_api_history_work", detail: "partial: incomplete API history"},
		{name: "legacy-empty", source: "api-user", history: "empty", wantGlobal: "partial", wantTop: "partial", reason: "retained_api_history_work", detail: "partial: incomplete API history"},
		{name: "complete", source: "api-user", history: "complete", wantGlobal: "full", wantTop: "full", detail: "user auth available for replies"},
		{name: "missing-record", wantGlobal: "full", wantTop: "full", detail: "user auth available for replies"},
		{name: "missing-database", missingDB: true, wantGlobal: "full", wantTop: "full", detail: "user auth available for replies"},
		{name: "thread-only", thread: true, wantGlobal: "partial", wantTop: "partial", reason: "retained_api_thread_work", detail: "partial: retained API thread skips or pending work"},
		{name: "both", source: "api-user", history: "pending", thread: true, wantGlobal: "partial", wantTop: "partial", reason: "retained_api_thread_work", detail: "partial: retained API thread skips or pending work"},
		{name: "global-partial-named-full", source: "api-user", history: "pending", named: true, globalPartial: true, wantGlobal: "partial", wantTop: "partial", detail: "partial without user auth"},
		{name: "global-full-named-partial", source: "api-user", history: "pending", named: true, namedPartial: true, wantGlobal: "partial", wantTop: "partial", reason: "retained_api_history_work", detail: "partial: incomplete API history"},
		{name: "both-partial-no-full-decision", source: "api-user", history: "bad-key", named: true, globalPartial: true, namedPartial: true, wantGlobal: "partial", wantTop: "partial", detail: "partial without user auth"},
		{name: "malformed-key-with-thread", source: "api-user", history: "bad-key", thread: true, wantError: true},
		{name: "foreign-malformed-value", source: "api-user", history: "bad-value", wantError: true},
		{name: "unrelated-source", source: "mcp", history: "bad-key", wantGlobal: "full", wantTop: "full", detail: "user auth available for replies"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, path := userPrimaryConfig(t)
			cfg.Sync.IncludeDMs = new(false)
			t.Setenv(cfg.Slack.Bot.TokenEnv, "fixture-global-bot")
			rolesWant := []string{"fixture-global-bot"}
			if !tc.globalPartial {
				t.Setenv(cfg.Slack.User.TokenEnv, "fixture-global-user")
				rolesWant = append(rolesWant, "fixture-global-user")
			}
			if tc.named {
				cfg.Workspaces = []config.Workspace{{ID: "TNAMED", BotTokenEnv: "SLACRAWL_COVERAGE_NAMED_BOT", UserTokenEnv: "SLACRAWL_COVERAGE_NAMED_USER"}}
				for _, kind := range []string{"BOT", "USER", "APP"} {
					t.Setenv("SLACK_TNAMED_"+kind+"_TOKEN", "")
				}
				t.Setenv("SLACRAWL_COVERAGE_NAMED_BOT", "fixture-named-bot")
				t.Setenv("SLACRAWL_COVERAGE_NAMED_USER", "")
				rolesWant = append(rolesWant, "fixture-named-bot")
				if !tc.namedPartial {
					t.Setenv("SLACRAWL_COVERAGE_NAMED_USER", "fixture-named-user")
					rolesWant = append(rolesWant, "fixture-named-user")
				} else if !tc.globalPartial {
					// Empty named credentials fall back to the global user token,
					// whose workspace mismatch keeps named auth unavailable.
					rolesWant = append(rolesWant, "fixture-global-user")
				}
			}
			require.NoError(t, cfg.Save(path))
			configBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			if !tc.missingDB {
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "stored-status"))
				if tc.thread {
					require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "TOLD|COLD|1", "missing_scope"))
				}
				if tc.history != "" {
					key := "[\"TOLD\",\"COLD\",\"\"]"
					raw := "{\"complete\":false,\"latest\":\"\",\"pending\":\"\"}"
					switch tc.history {
					case "empty":
						raw = "{\"complete\":false,\"latest\":\"\"}"
					case "complete":
						raw = "{\"complete\":true,\"latest\":\"1710000000.000000\"}"
					case "bad-key":
						key = "private-history-key-canary"
					case "bad-value":
						raw = "private-history-value-canary"
					}
					require.NoError(t, st.SetSyncState(ctx, tc.source, store.APIHistoryEntityType, key, raw))
				}
				require.NoError(t, st.Close())
			}
			before := shareArchiveSnapshot(t, cfg.DBPath)
			var output bytes.Buffer
			var roles []string
			app := &App{Stdout: &output, Stderr: &output, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "/auth.test", r.URL.Path)
				role := cliSlackToken(r)
				roles = append(roles, role)
				team := "TGLOBAL"
				if role == "fixture-named-bot" || role == "fixture-named-user" {
					team = "TNAMED"
				}
				body, err := json.Marshal(map[string]any{"ok": true, "team_id": team})
				require.NoError(t, err)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
			})}}
			for _, format := range []string{"json", "text", "log"} {
				output.Reset()
				roles = nil
				err := app.Run(ctx, []string{"--config", path, "--format", format, "doctor"})
				require.Equal(t, rolesWant, roles)
				if tc.wantError {
					require.Error(t, err)
					require.Contains(t, err.Error(), "invalid API history checkpoint")
					require.NotContains(t, err.Error(), "canary")
					require.Empty(t, output.String())
				} else {
					require.NoError(t, err)
					switch format {
					case "json":
						var report map[string]any
						require.NoError(t, json.Unmarshal(output.Bytes(), &report))
						require.Equal(t, tc.wantTop, report["thread_coverage"])
						diag := report["slack_api"].(map[string]any)
						require.Equal(t, tc.wantGlobal, diag["thread_coverage"])
						require.Equal(t, !tc.globalPartial, diag["user_auth_available"])
						if tc.reason == "" {
							require.NotContains(t, diag, "thread_coverage_reason")
						} else {
							require.Equal(t, tc.reason, diag["thread_coverage_reason"])
						}
						if tc.named {
							namedReport := report["workspace_api"].([]any)[0].(map[string]any)
							named := namedReport["slack_api"].(map[string]any)
							require.Equal(t, map[bool]string{true: "partial", false: "full"}[tc.namedPartial], named["thread_coverage"])
							require.Equal(t, !tc.namedPartial, named["user_auth_available"])
							require.Equal(t, !tc.namedPartial || !tc.globalPartial, namedReport["tokens"].(map[string]any)["user_set"])
							if tc.namedPartial && !tc.globalPartial {
								require.Equal(t, "authenticated workspace TGLOBAL does not match requested workspace TNAMED", named["user_auth_error"])
							} else {
								require.NotContains(t, named, "user_auth_error")
							}
							require.NotContains(t, named, "thread_coverage_reason")
						}
						if !tc.missingDB {
							require.Equal(t, "stored-status", report["status"].(map[string]any)["thread_state"])
						}
					case "text":
						require.Contains(t, output.String(), tc.detail)
					case "log":
						reason := tc.reason
						if reason == "" {
							reason = "-"
						}
						require.Contains(t, output.String(), "thread_coverage_reason=\""+reason+"\"")
					}
				}
				require.NotContains(t, output.String(), "private-history-key-canary")
				require.NotContains(t, output.String(), "private-history-value-canary")
				require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
				configAfter, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, configBefore, configAfter)
				if tc.missingDB {
					require.False(t, sharePathExists(t, cfg.DBPath))
				}
			}
		})
	}
}
