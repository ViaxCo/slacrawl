package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestAPIDMPolicyFromCLIConfig(t *testing.T) {
	for _, setting := range []*bool{nil, new(true), new(false)} {
		for _, userToken := range []bool{false, true} {
			name := "omitted"
			if setting != nil {
				name = fmt.Sprint(*setting)
			}
			t.Run(fmt.Sprintf("%s/user=%v", name, userToken), func(t *testing.T) {
				dir := t.TempDir()
				cfg := config.Default()
				cfg.DBPath = filepath.Join(dir, "archive.db")
				cfg.CacheDir = filepath.Join(dir, "cache")
				cfg.LogDir = filepath.Join(dir, "logs")
				cfg.Share.RepoPath = filepath.Join(dir, "share")
				cfg.Slack.Desktop.Enabled = false
				cfg.Slack.App.Enabled = false
				cfg.Slack.Bot.TokenEnv = "SLACRAWL_ADMISSION_BOT"
				cfg.Slack.User.TokenEnv = "SLACRAWL_ADMISSION_USER"
				cfg.Slack.User.Enabled = userToken
				cfg.Sync.IncludeDMs = setting
				t.Setenv("SLACRAWL_ADMISSION_BOT", "fixture-bot")
				t.Setenv("SLACRAWL_ADMISSION_USER", "fixture-user")
				configPath := filepath.Join(dir, "config.toml")
				require.NoError(t, cfg.Save(configPath))
				var dmLists, dmHistory atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					values := cliSlackForm(r)
					w.Header().Set("Content-Type", "application/json")
					switch r.URL.Path {
					case "/auth.test":
						_, _ = w.Write([]byte(`{"ok":true,"team_id":"T123","team":"Fixture"}`))
					case "/conversations.list":
						if strings.Contains(values.Get("types"), "im") {
							dmLists.Add(1)
							_, _ = w.Write([]byte(`{"ok":true,"channels":[]}`))
						} else {
							_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C123","is_channel":true},{"id":"CDM","is_im":true,"name":"dm-canary"}]}`))
						}
					case "/conversations.history":
						if values.Get("channel") == "CDM" {
							dmHistory.Add(1)
						}
						_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","ts":"1710000000.000000","text":"fixture"}]}`))
					case "/users.list":
						_, _ = w.Write([]byte(`{"ok":true,"members":[]}`))
					default:
						t.Errorf("unexpected request %s", r.URL.Path)
						http.NotFound(w, r)
					}
				}))
				defer server.Close()
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output, apiURL: server.URL + "/", httpClient: server.Client()}
				require.NoError(t, app.Run(context.Background(), []string{"--config", configPath, "--json", "sync", "--source", "api"}))
				st, err := store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				defer st.Close()
				rows, err := st.QueryReadOnly(context.Background(), "select channel_id from messages order by channel_id")
				require.NoError(t, err)
				if setting != nil && !*setting {
					require.Equal(t, []map[string]any{{"channel_id": "C123"}}, rows)
					require.Zero(t, dmLists.Load())
					require.Zero(t, dmHistory.Load())
					require.NotContains(t, output.String(), "dm-canary")
				} else {
					require.Len(t, rows, 2, "omitted/true preserve acceptance of returned DM metadata")
					require.Equal(t, int32(1), dmHistory.Load())
					if userToken {
						require.Positive(t, dmLists.Load())
					} else {
						require.Zero(t, dmLists.Load())
					}
				}
			})
		}
	}
}
