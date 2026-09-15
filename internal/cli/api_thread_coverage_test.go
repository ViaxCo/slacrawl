package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestStatusProjectsRetainedAPIHistoryReadOnly(t *testing.T) {
	for _, mode := range []string{"full", "partial", "other-marker", "malformed", "missing-db"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cfg, path := userPrimaryConfig(t)
			require.NoError(t, cfg.Save(path))
			configBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			want := "partial"
			if mode == "missing-db" {
				want = ""
			} else {
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				marker := "full"
				if mode == "partial" {
					marker = "partial"
				} else if mode == "other-marker" {
					marker, want = "stored-status", "stored-status"
				}
				require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", marker))
				raw := `{"complete":false,"latest":"","pending":""}`
				if mode == "malformed" {
					raw = "private-history-canary"
				}
				require.NoError(t, st.SetSyncState(ctx, "api-bot", store.APIHistoryEntityType, `["T1","C1",""]`, raw))
				require.NoError(t, st.Close())
			}
			before := shareArchiveSnapshot(t, cfg.DBPath)
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("status must not issue API requests")
				return nil, nil
			})}}
			for _, format := range []string{"json", "text", "log"} {
				output.Reset()
				err := app.Run(ctx, []string{"--config", path, "--format", format, "status"})
				if mode == "malformed" {
					require.EqualError(t, err, "invalid API history checkpoint")
					require.Empty(t, output.String())
				} else {
					require.NoError(t, err)
					switch format {
					case "json":
						var report map[string]any
						require.NoError(t, json.Unmarshal(output.Bytes(), &report))
						require.Equal(t, want, report["thread_state"])
						require.NotContains(t, report, "api_thread_coverage")
					case "text":
						display := want
						if display == "" {
							display = "-"
						}
						require.Contains(t, output.String(), "thread state "+display)
					case "log":
						display := want
						if display == "" {
							display = "-"
						}
						require.Contains(t, output.String(), "thread_state=\""+display+"\"")
					}
				}
				require.NotContains(t, output.String(), "private-history-canary")
				require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
				configAfter, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, configBefore, configAfter)
				if mode == "missing-db" {
					require.False(t, sharePathExists(t, cfg.DBPath))
				}
			}
		})
	}
}

func TestSyncResponseProjectsForeignRetainedHistory(t *testing.T) {
	ctx := context.Background()
	cfg, path := userPrimaryConfig(t)
	cfg.WorkspaceID = "T1"
	cfg.Sync.IncludeDMs = new(false)
	t.Setenv(cfg.Slack.User.TokenEnv, "fixture-user")
	require.NoError(t, cfg.Save(path))
	configBefore, err := os.ReadFile(path)
	require.NoError(t, err)
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	_, err = st.DB().ExecContext(ctx, "insert into sync_state values ('doctor','threads','coverage','full','2000-01-01T00:00:00Z')")
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(ctx, "api-bot", store.APIHistoryEntityType, `["TOTHER","COTHER",""]`, `{"complete":false,"latest":"","pending":""}`))
	const preservedQuery = "select * from sync_state where source_name='doctor' or (source_name='api-bot' and entity_type='history_coverage_v1') order by source_name,entity_id"
	before, err := st.QueryReadOnly(ctx, preservedQuery)
	require.NoError(t, err)
	var output, stderr bytes.Buffer
	var calls []string
	app := &App{Stdout: &output, Stderr: &stderr, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: cliRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.URL.Path)
		require.Equal(t, "fixture-user", cliSlackToken(r))
		var body string
		switch r.URL.Path {
		case "/auth.test":
			body = `{"ok":true,"team_id":"T1"}`
		case "/conversations.list":
			body = `{"ok":true,"channels":[{"id":"C1","is_channel":true,"name":"fixture"}]}`
		case "/conversations.history":
			body = `{"ok":true,"messages":[]}`
		case "/users.list":
			body = `{"ok":true,"members":[]}`
		default:
			t.Fatalf("unexpected API request %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewBufferString(body)), Request: r}, nil
	})}}
	require.NoError(t, app.Run(ctx, []string{"--config", path, "--json", "sync", "--source", "api", "--with-media=false"}))
	require.Equal(t, []string{"/auth.test", "/conversations.list", "/conversations.history", "/users.list"}, calls)
	var report map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &report))
	require.Equal(t, "partial", report["status"].(map[string]any)["thread_state"])
	after, err := st.QueryReadOnly(ctx, preservedQuery)
	require.NoError(t, err)
	require.Equal(t, before, after)
	configAfter, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, configBefore, configAfter)
}
