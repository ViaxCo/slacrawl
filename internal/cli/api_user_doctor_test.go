package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

// Candidate-only: bot-bearing Doctor must never run on the parent, which
// ignores the App endpoint seam. The baseline file contains only bot-free Doctor.
func TestDoctorGlobalAndNamedCoverage(t *testing.T) {
	for _, tc := range []struct {
		name                                             string
		globalFull, globalInvalid, namedPartial, noNamed bool
		missingDB, retainedSkip, otherSkip, unsafeError  bool
		wantGlobal, wantTop                              string
		wantReason, wantText                             string
	}{
		{name: "global-partial/named-full/missing", missingDB: true, wantGlobal: "partial", wantTop: "full", wantText: "partial without user auth"},
		{name: "global-full/named-partial/empty", globalFull: true, namedPartial: true, wantGlobal: "full", wantTop: "partial", wantText: "user auth available for replies"},
		{name: "global-partial/named-full/skipped", retainedSkip: true, wantGlobal: "partial", wantTop: "partial", wantText: "partial without user auth"},
		{name: "global-full/named-partial/skipped", globalFull: true, namedPartial: true, retainedSkip: true, wantGlobal: "partial", wantTop: "partial", wantReason: "retained_api_thread_work", wantText: "partial: retained API thread skips or pending work"},
		{name: "global-full/no-named/skipped", globalFull: true, noNamed: true, retainedSkip: true, wantGlobal: "partial", wantTop: "partial", wantReason: "retained_api_thread_work", wantText: "partial: retained API thread skips or pending work"},
		{name: "global-full/named-full/unrelated-skips", globalFull: true, otherSkip: true, wantGlobal: "full", wantTop: "full", wantText: "user auth available for replies"},
		{name: "global-invalid/named-full/missing", globalInvalid: true, missingDB: true, wantGlobal: "partial", wantTop: "full", wantText: "partial without user auth"},
		{name: "global-invalid/named-full/skipped", globalInvalid: true, retainedSkip: true, wantGlobal: "partial", wantTop: "partial", wantText: "partial without user auth"},
		{name: "global-unsafe/named-unsafe/missing", globalInvalid: true, namedPartial: true, unsafeError: true, missingDB: true, wantGlobal: "partial", wantTop: "partial", wantText: "partial without user auth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, configPath := userPrimaryConfig(t)
			cfg.Sync.IncludeDMs = new(false)
			t.Setenv(cfg.Slack.Bot.TokenEnv, "global-bot")
			teamByRole := map[string]string{"global-bot": "TGLOBAL"}
			wantRoles := []string{"global-bot"}
			if tc.globalFull || tc.globalInvalid {
				t.Setenv(cfg.Slack.User.TokenEnv, "global-user")
				if tc.globalFull {
					teamByRole["global-user"] = "TGLOBAL"
				}
				wantRoles = append(wantRoles, "global-user")
			}
			if !tc.noNamed {
				cfg.Workspaces = []config.Workspace{
					{ID: "T1", BotTokenEnv: "SLACRAWL_DOCTOR_T1_BOT", UserTokenEnv: "SLACRAWL_DOCTOR_T1_USER"},
					{ID: "T2", BotTokenEnv: "SLACRAWL_DOCTOR_T2_BOT", UserTokenEnv: "SLACRAWL_DOCTOR_T2_USER"},
				}
				for _, workspace := range cfg.Workspaces {
					for _, kind := range []string{"BOT", "USER", "APP"} {
						t.Setenv("SLACK_"+workspace.ID+"_"+kind+"_TOKEN", "")
					}
					bot, user := workspace.ID+"-bot", workspace.ID+"-user"
					t.Setenv(workspace.BotTokenEnv, bot)
					t.Setenv(workspace.UserTokenEnv, user)
					teamByRole[bot] = workspace.ID
					if workspace.ID != "T2" || !tc.namedPartial {
						teamByRole[user] = workspace.ID
					}
					wantRoles = append(wantRoles, bot, user)
				}
			}
			require.NoError(t, cfg.Save(configPath))
			if !tc.missingDB {
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				if tc.retainedSkip || tc.otherSkip {
					require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "retained-status"))
				}
				if tc.retainedSkip {
					// Archive-wide retained omissions need not belong to a configured workspace.
					require.NoError(t, st.SetSyncState(ctx, "api-user", "thread_skip", "TOLD|COLD|1", "not_in_channel"))
				}
				if tc.otherSkip {
					require.NoError(t, st.SetSyncState(ctx, "mcp", "thread_skip", "TOLD|COLD|1", "unrelated"))
					require.NoError(t, st.SetSyncState(ctx, "api-bot", "thread_skip", "TOLD|COLD|2", "unrelated"))
					require.NoError(t, st.SetSyncState(ctx, "api-user", "channel_skip", "COLD", "unrelated"))
				}
				require.NoError(t, st.Close())
			}
			before := shareArchiveSnapshot(t, cfg.DBPath)
			authCode, authDisplay := "invalid_auth", "invalid_auth"
			if tc.unsafeError {
				authCode, authDisplay = "doctor-error-canary", "slack auth.test API response failed"
			}
			var roles, problems []string
			transport := cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				role := cliSlackToken(r)
				roles = append(roles, role)
				if r.URL.Path != "/auth.test" {
					problems = append(problems, "unexpected non-auth request")
				}
				payload := map[string]any{"ok": true, "team_id": teamByRole[role], "team": "Fixture " + teamByRole[role]}
				if teamByRole[role] == "" {
					payload = map[string]any{"ok": false, "error": authCode}
				}
				data, err := json.Marshal(payload)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
			})
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: transport}}
			require.NoError(t, app.Run(ctx, []string{"--config", configPath, "--json", "doctor"}))
			require.NotContains(t, output.String(), "doctor-error-canary")
			var report map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &report))
			diag := report["slack_api"].(map[string]any)
			require.Equal(t, tc.wantGlobal, diag["thread_coverage"])
			if tc.wantReason == "" {
				require.NotContains(t, diag, "thread_coverage_reason")
			} else {
				require.Equal(t, tc.wantReason, diag["thread_coverage_reason"])
			}
			require.Equal(t, tc.wantTop, report["thread_coverage"])
			require.Equal(t, "TGLOBAL", diag["bot_auth_team_id"])
			require.Equal(t, "Fixture TGLOBAL", diag["bot_auth_team"])
			require.Equal(t, true, diag["bot_configured"])
			require.Equal(t, tc.globalFull || tc.globalInvalid, diag["user_configured"])
			require.Equal(t, tc.globalFull, diag["user_auth_available"])
			if tc.globalInvalid {
				require.Equal(t, authDisplay, diag["user_auth_error"])
			} else {
				require.Nil(t, diag["user_auth_error"])
			}
			require.Equal(t, false, diag["app_tail_available"])
			if !tc.noNamed {
				named := report["workspace_api"].([]any)
				require.Len(t, named, 2)
				for i, workspace := range named {
					d := workspace.(map[string]any)["slack_api"].(map[string]any)
					require.NotContains(t, d, "thread_coverage_reason")
					full := i == 0 || !tc.namedPartial
					require.Equal(t, []string{"T1", "T2"}[i], d["bot_auth_team_id"])
					require.Equal(t, map[bool]string{true: "full", false: "partial"}[full], d["thread_coverage"])
					require.Equal(t, full, d["user_auth_available"])
					if !full {
						require.Equal(t, authDisplay, d["user_auth_error"])
					}
				}
			}
			require.Equal(t, wantRoles, roles)
			require.Empty(t, problems)
			require.True(t, reflect.DeepEqual(before, shareArchiveSnapshot(t, cfg.DBPath)))
			if tc.retainedSkip || tc.otherSkip {
				require.Equal(t, "retained-status", report["status"].(map[string]any)["thread_state"])
			}
			for _, format := range []string{"text", "log"} {
				output.Reset()
				roles, problems = nil, nil
				require.NoError(t, app.Run(ctx, []string{"--config", configPath, "--format", format, "doctor"}))
				require.NotContains(t, output.String(), "doctor-error-canary")
				if format == "text" {
					require.Contains(t, output.String(), tc.wantText)
					if tc.globalFull {
						require.NotContains(t, output.String(), "partial without user auth")
					} else {
						require.NotContains(t, output.String(), "user auth available for replies")
						require.NotContains(t, output.String(), "retained API thread skips or pending work")
					}
				} else {
					var globalLine string
					for _, line := range strings.Split(output.String(), "\n") {
						if strings.HasPrefix(line, "doctor.slack_api ") {
							require.Empty(t, globalLine)
							globalLine = line
						}
					}
					wantReason := tc.wantReason
					if wantReason == "" {
						wantReason = "-"
					}
					require.Contains(t, globalLine, `thread_coverage="`+tc.wantGlobal+`"`)
					require.Contains(t, globalLine, `thread_coverage_reason="`+wantReason+`"`)
				}
				require.Equal(t, wantRoles, roles)
				require.Empty(t, problems)
				require.True(t, reflect.DeepEqual(before, shareArchiveSnapshot(t, cfg.DBPath)))
				if tc.missingDB {
					require.False(t, sharePathExists(t, cfg.DBPath))
				}
			}
		})
	}
}
