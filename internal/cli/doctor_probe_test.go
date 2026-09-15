package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestDoctorDMProbeReportsFromCLI(t *testing.T) {
	for _, mode := range []string{"global-catalog", "named-catalog", "global-catalog-retained", "named-catalog-retained", "empty", "scope-only"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cfg, path := userPrimaryConfig(t)
			cfg.Sync.IncludeDMs = new(true)
			t.Setenv(cfg.Slack.Bot.TokenEnv, "global-bot")
			t.Setenv(cfg.Slack.User.TokenEnv, "global-user")
			cfg.Workspaces = []config.Workspace{{ID: "T1", BotTokenEnv: "SLACRAWL_PROBE_T1_BOT", UserTokenEnv: "SLACRAWL_PROBE_T1_USER"}}
			t.Setenv("SLACRAWL_PROBE_T1_BOT", "named-bot")
			t.Setenv("SLACRAWL_PROBE_T1_USER", "named-user")
			t.Setenv("SLACK_T1_APP_TOKEN", "")
			require.NoError(t, cfg.Save(path))
			configBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			userPrimarySeed(t, cfg.DBPath, []string{"TGLOBAL", "T1"})
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "stored-status"))
			retained := strings.HasSuffix(mode, "-retained")
			if retained {
				require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "TOLD|COLD|1", "not_in_channel"))
			}
			require.NoError(t, st.Close())
			before := shareArchiveSnapshot(t, cfg.DBPath)
			globalFailure, namedFailure := "catalog_failed", "history_failed"
			if strings.HasPrefix(mode, "named-catalog") {
				globalFailure, namedFailure = namedFailure, globalFailure
			}
			if mode == "empty" || mode == "scope-only" {
				globalFailure, namedFailure = "", ""
			}
			var calls []string
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				form := cliSlackForm(r)
				role := cliSlackToken(r)
				calls = append(calls, r.URL.Path+":"+role+":"+form.Get("channel"))
				team, failure := "TGLOBAL", globalFailure
				if strings.HasPrefix(role, "named-") {
					team, failure = "T1", namedFailure
				}
				var payload any
				switch r.URL.Path {
				case "/auth.test":
					payload = map[string]any{"ok": true, "team_id": team}
				case "/conversations.list":
					require.Equal(t, team, form.Get("team_id"))
					require.Equal(t, "im,mpim", form.Get("types"))
					if failure == "catalog_failed" {
						payload = map[string]any{"ok": false, "error": "probe-private-canary"}
					} else if mode == "empty" {
						payload = map[string]any{"ok": true, "channels": []any{}}
					} else {
						payload = json.RawMessage(`{"ok":true,"channels":[{"id":"D1","is_im":true},{"id":"G1","is_mpim":true}]}`)
					}
				case "/conversations.history":
					require.Equal(t, "1", form.Get("limit"))
					require.Contains(t, []string{"D1", "G1"}, form.Get("channel"))
					code := "probe-private-canary"
					if form.Get("channel") == "D1" || mode == "scope-only" {
						code = "missing_scope"
					}
					payload = map[string]any{"ok": false, "error": code}
				default:
					t.Fatalf("unexpected request %s", r.URL.Path)
				}
				data, err := json.Marshal(payload)
				require.NoError(t, err)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
			})}}
			for _, format := range []string{"json", "log", "text"} {
				output.Reset()
				calls = nil
				require.NoError(t, app.Run(ctx, []string{"--config", path, "--format", format, "doctor"}))
				require.NotContains(t, output.String(), "probe-private-canary")
				wantCalls := []string{}
				for _, prefix := range []string{"global", "named"} {
					failure := globalFailure
					if prefix == "named" {
						failure = namedFailure
					}
					wantCalls = append(wantCalls, "/auth.test:"+prefix+"-bot:", "/auth.test:"+prefix+"-user:", "/conversations.list:"+prefix+"-user:")
					if failure == "history_failed" || mode == "scope-only" {
						wantCalls = append(wantCalls, "/conversations.history:"+prefix+"-user:D1", "/conversations.history:"+prefix+"-user:G1")
					}
				}
				require.Equal(t, wantCalls, calls)
				if format == "json" {
					var report map[string]any
					require.NoError(t, json.Unmarshal(output.Bytes(), &report))
					global := report["slack_api"].(map[string]any)
					named := report["workspace_api"].([]any)[0].(map[string]any)["slack_api"].(map[string]any)
					for i, diag := range []map[string]any{global, named} {
						failure := []string{globalFailure, namedFailure}[i]
						if failure == "" {
							require.NotContains(t, diag, "dm_probe_error")
						} else {
							require.Equal(t, failure, diag["dm_probe_error"])
						}
						if mode == "scope-only" {
							require.Equal(t, "im:history,mpim:history", diag["dms_missing_scope"])
						} else if failure == "history_failed" {
							require.Equal(t, "im:history", diag["dms_missing_scope"])
						} else {
							require.NotContains(t, diag, "dms_missing_scope")
						}
						require.Equal(t, true, diag["user_auth_available"])
						require.NotContains(t, diag, "user_auth_error")
						require.Equal(t, true, diag["dms_included"])
					}
					wantCoverage := "full"
					if retained {
						wantCoverage = "partial"
					}
					require.Equal(t, wantCoverage, report["thread_coverage"])
					require.Equal(t, wantCoverage, global["thread_coverage"])
					require.Equal(t, "full", named["thread_coverage"])
					if retained {
						require.Equal(t, "retained_api_thread_work", global["thread_coverage_reason"])
					} else {
						require.NotContains(t, global, "thread_coverage_reason")
					}
					require.NotContains(t, named, "thread_coverage_reason")
					require.Equal(t, "stored-status", report["status"].(map[string]any)["thread_state"])
				} else if format == "text" {
					require.Contains(t, output.String(), "DM access (T1)")
					require.NotContains(t, output.String(), "missing scope: -")
					require.NotContains(t, output.String(), "full historical replies")
					if mode == "empty" {
						require.Equal(t, 2, strings.Count(output.String(), "enabled for user token; history coverage not verified"))
					} else if mode == "scope-only" {
						require.Equal(t, 2, strings.Count(output.String(), "missing scope: im:history,mpim:history"))
						require.NotContains(t, output.String(), "check failed")
					} else {
						require.Contains(t, output.String(), "DM catalog check failed; retry doctor to check access")
						require.Contains(t, output.String(), "missing scope: im:history; DM history check failed; retry doctor to check access")
					}
					if retained {
						require.Contains(t, output.String(), "partial: retained API thread skips or pending work")
					} else {
						require.Contains(t, output.String(), "user auth available for replies")
					}
				} else {
					if mode == "empty" || mode == "scope-only" {
						require.Contains(t, output.String(), `dm_probe_error="-"`)
					} else {
						require.Contains(t, output.String(), `dm_probe_error="catalog_failed"`)
						require.Contains(t, output.String(), `dm_probe_error="history_failed"`)
					}
				}
				require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
				configAfter, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, configBefore, configAfter)
			}
		})
	}
}

func TestDoctorCallerCancellationFromCLI(t *testing.T) {
	for _, scope := range []string{"global", "named"} {
		for _, phase := range []string{"optional-auth", "catalog", "history"} {
			t.Run(scope+"/"+phase, func(t *testing.T) {
				cfg, path := userPrimaryConfig(t)
				cfg.Sync.IncludeDMs = new(true)
				t.Setenv(cfg.Slack.Bot.TokenEnv, "global-bot")
				t.Setenv(cfg.Slack.User.TokenEnv, "global-user")
				cfg.Workspaces = []config.Workspace{{ID: "T1", BotTokenEnv: "SLACRAWL_PROBE_T1_BOT", UserTokenEnv: "SLACRAWL_PROBE_T1_USER"}}
				t.Setenv("SLACRAWL_PROBE_T1_BOT", "named-bot")
				t.Setenv("SLACRAWL_PROBE_T1_USER", "named-user")
				t.Setenv("SLACK_T1_APP_TOKEN", "")
				require.NoError(t, cfg.Save(path))
				before, err := os.ReadFile(path)
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				var stdout, stderr bytes.Buffer
				app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					role := cliSlackToken(r)
					form := cliSlackForm(r)
					method := r.URL.Path
					targetMethod := map[string]string{"optional-auth": "/auth.test", "catalog": "/conversations.list", "history": "/conversations.history"}[phase]
					if role == scope+"-user" && method == targetMethod {
						cancel()
					}
					team := "TGLOBAL"
					if strings.HasPrefix(role, "named-") {
						team = "T1"
					}
					payload := map[string]any{"ok": true}
					switch method {
					case "/auth.test":
						payload["team_id"] = team
					case "/conversations.list":
						payload["channels"] = []any{map[string]any{"id": "D1", "is_im": true}, map[string]any{"id": "G1", "is_mpim": true}}
					case "/conversations.history":
						require.Equal(t, "1", form.Get("limit"))
						payload["messages"] = []any{}
					default:
						t.Fatalf("unexpected request %s", method)
					}
					data, err := json.Marshal(payload)
					require.NoError(t, err)
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
				})}}
				err = app.Run(ctx, []string{"--config", path, "--json", "doctor"})
				require.ErrorIs(t, err, context.Canceled)
				want := map[string]int{"optional-auth": 2, "catalog": 3, "history": 4}[phase]
				if scope == "named" {
					want += 5
				}
				require.Equal(t, want, calls)
				require.Empty(t, stdout.String())
				require.Empty(t, stderr.String())
				require.False(t, sharePathExists(t, cfg.DBPath))
				require.False(t, cfg.Slack.Desktop.Enabled)
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		}
	}
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("entry/deadline=%t", deadline), func(t *testing.T) {
			cfg, path := userPrimaryConfig(t)
			require.NoError(t, cfg.Save(path))
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := error(context.Canceled)
			if deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			var out bytes.Buffer
			app := &App{Stdout: &out, Stderr: &out, httpClient: &http.Client{Transport: cliRoundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("canceled caller must not request Slack")
				return nil, nil
			})}}
			require.ErrorIs(t, app.Run(ctx, []string{"--config", path, "--json", "doctor"}), want)
			require.Empty(t, out.String())
			require.False(t, sharePathExists(t, cfg.DBPath))
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
