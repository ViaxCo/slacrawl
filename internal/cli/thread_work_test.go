package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/store"
)

func TestPendingThreadDoctorAndFreshness(t *testing.T) {
	for _, tc := range []struct {
		name, source, profile, wantCoverage string
		realTimestamp                       bool
		wantReason, wantText                string
	}{
		{"api-only", "api-user", "bot", "partial", false, "retained_api_thread_work", "partial: retained API thread skips or pending work"},
		{"mcp-only", "mcp", "mcp", "full", false, "", "full historical replies"},
		{"api-with-success", "api-user", "bot", "partial", true, "retained_api_thread_work", "partial: retained API thread skips or pending work"},
		{"unrelated-source", "api-bot", "bot", "full", false, "", "full historical replies"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, path := userPrimaryConfig(t)
			cfg.Sync.IncludeDMs = new(false)
			t.Setenv(cfg.Slack.Bot.TokenEnv, "fixture-bot")
			t.Setenv(cfg.Slack.User.TokenEnv, "fixture-user")
			require.NoError(t, cfg.Save(path))
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			_, err = st.DB().ExecContext(ctx, "insert into sync_state values (?, 'thread_pending_v1', '[\"T1\",\"C1\",\"1\"]', 'pending', '2099-01-01T00:00:00Z')", tc.source)
			require.NoError(t, err)
			if tc.realTimestamp {
				_, err = st.DB().ExecContext(ctx, "insert into sync_state values (?, 'workspace', 'T1', 'old', '2000-01-01T00:00:00Z')", tc.source)
				require.NoError(t, err)
			}
			require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "stored-status"))
			require.NoError(t, st.Close())
			before := shareArchiveSnapshot(t, cfg.DBPath)
			var output bytes.Buffer
			var roles []string
			app := &App{Stdout: &output, Stderr: &output, apiURL: "https://thread-doctor.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "/auth.test", r.URL.Path)
				roles = append(roles, cliSlackToken(r))
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true,"team_id":"T1","team":"Fixture"}`)), Request: r}, nil
			})}}
			require.NoError(t, app.Run(ctx, []string{"--config", path, "--json", "doctor"}))
			var report map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &report))
			require.Equal(t, tc.wantCoverage, report["thread_coverage"])
			diag := report["slack_api"].(map[string]any)
			require.Equal(t, tc.wantCoverage, diag["thread_coverage"])
			require.Equal(t, true, diag["user_auth_available"])
			if tc.wantReason == "" {
				require.NotContains(t, diag, "thread_coverage_reason")
			} else {
				require.Equal(t, tc.wantReason, diag["thread_coverage_reason"])
			}
			require.Equal(t, "stored-status", report["status"].(map[string]any)["thread_state"])
			require.Equal(t, []string{"fixture-bot", "fixture-user"}, roles)
			require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
			for _, format := range []string{"text", "log"} {
				output.Reset()
				roles = nil
				require.NoError(t, app.Run(ctx, []string{"--config", path, "--format", format, "doctor"}))
				if format == "text" {
					require.Contains(t, output.String(), tc.wantText)
					require.NotContains(t, output.String(), "partial without user auth")
				} else {
					wantReason := tc.wantReason
					if wantReason == "" {
						wantReason = "-"
					}
					require.Contains(t, output.String(), `thread_coverage_reason="`+wantReason+`"`)
				}
				require.Equal(t, []string{"fixture-bot", "fixture-user"}, roles)
				require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
			}
			st, err = store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			profile, err := app.buildArchiveProfile(ctx, cfg, st)
			require.NoError(t, err)
			require.NoError(t, st.Close())
			found := false
			for _, source := range profile.Sources {
				if source.Name != tc.profile {
					continue
				}
				found = true
				wantTime, wantCount := "", int64(1)
				if tc.realTimestamp {
					wantTime, wantCount = "2000-01-01T00:00:00Z", 2
				} else if tc.source == "api-bot" {
					wantTime = "2099-01-01T00:00:00Z"
				}
				require.Equal(t, wantTime, source.LastSeenAt)
				require.Equal(t, wantCount, source.SyncEntries)
			}
			require.True(t, found)
		})
	}
}
