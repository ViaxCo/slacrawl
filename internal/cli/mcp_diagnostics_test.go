package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMCPWorkspaceCollisionDiagnosticsAcrossCLIPaths(t *testing.T) {
	const foreignWorkspace = "TCANARYFOREIGN"
	const foreignChannel = "CCANARYFOREIGN"
	const foreignUser = "UCANARYFOREIGN"
	const canary = "synthetic-private-collision-content"
	for _, source := range []string{"mcp", "connector"} {
		for _, collision := range []string{"user", "channel", "message"} {
			t.Run(source+"/"+collision, func(t *testing.T) {
				dir := t.TempDir()
				cfg := config.Default()
				cfg.DBPath = filepath.Join(dir, "archive.db")
				cfg.CacheDir = filepath.Join(dir, "cache")
				cfg.LogDir = filepath.Join(dir, "logs")
				cfg.Share.RepoPath = filepath.Join(dir, "share")
				cfg.Share.AutoUpdate = false
				ctx := context.Background()
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Now().UTC()
				for _, workspace := range []string{"TREQUEST", foreignWorkspace} {
					require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: workspace, Name: "preserved", RawJSON: "{}", UpdatedAt: now}))
				}
				for _, channel := range []store.Channel{
					{ID: "CLOCAL", WorkspaceID: "TREQUEST", Kind: "public_channel", Name: "preserved", RawJSON: "{}", UpdatedAt: now},
					{ID: foreignChannel, WorkspaceID: foreignWorkspace, Kind: "public_channel", Name: "preserved", RawJSON: "{}", UpdatedAt: now},
				} {
					require.NoError(t, st.UpsertChannel(ctx, channel))
				}
				require.NoError(t, st.UpsertUser(ctx, store.User{ID: foreignUser, WorkspaceID: foreignWorkspace, Name: "preserved", RawJSON: "{}", UpdatedAt: now}))
				require.NoError(t, st.UpsertMessage(ctx, store.Message{
					WorkspaceID: foreignWorkspace, ChannelID: foreignChannel, TS: "1710000000.000001", UserID: foreignUser,
					Text: "preserved", NormalizedText: "preserved", SourceName: "api-user", SourceRank: 1, RawJSON: "{}", UpdatedAt: now,
				}, nil))
				require.NoError(t, st.SetSyncState(ctx, "mcp", "workspace", "TREQUEST", "previous-proof"))
				before := map[string][]map[string]any{}
				for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state"} {
					before[table], err = st.QueryReadOnly(ctx, "select * from "+table)
					require.NoError(t, err)
				}
				require.NoError(t, st.Close())

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var request struct {
						Method string `json:"method"`
						Params struct {
							Name string `json:"name"`
						} `json:"params"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					var result any
					switch request.Method {
					case "initialize":
						result = map[string]any{"protocolVersion": "2025-03-26"}
					case "notifications/initialized":
						w.WriteHeader(http.StatusAccepted)
						return
					case "tools/list":
						result = map[string]any{"tools": []map[string]any{{"name": "slack_search_channels"}, {"name": "slack_search_users"}, {"name": "slack_read_channel"}}}
					case "tools/call":
						channelID := "CLOCAL"
						if collision == "channel" {
							channelID = foreignChannel
						}
						payload := map[string]any{}
						switch request.Params.Name {
						case "slack_search_channels":
							payload["results"] = "### Result 1\nChannel ID: " + channelID + "\nName: test\nIs Private: false"
						case "slack_search_users":
							if collision == "user" {
								payload["results"] = "### Result 1\nUser ID: " + foreignUser + "\nName: test"
							}
						case "slack_read_channel":
							payload["messages"] = "Channel: test (" + channelID + ")"
							if collision == "message" {
								// Admission now rejects this foreign header before either
								// message can reach persistence.
								payload["messages"] = "Channel: test (" + foreignChannel + ")\n\n=== Message from test (" + foreignUser + ") at 2024-03-09T16:00:00Z === \nMessage TS: 1710000000.000002\n" + canary + "\n\n=== Message from test (" + foreignUser + ") at 2024-03-09T16:00:00Z === \nMessage TS: 1710000000.000001\n" + canary
							}
						default:
							t.Fatalf("unexpected tool %q", request.Params.Name)
						}
						raw, err := json.Marshal(payload)
						require.NoError(t, err)
						result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
					default:
						t.Fatalf("unexpected method %q", request.Method)
					}
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result}))
				}))
				defer server.Close()
				cfg.Slack.MCP.Enabled = true
				cfg.Slack.MCP.BaseURL = server.URL
				cfg.Slack.MCP.TokenEnv = "SLACRAWL_MCP_DIAGNOSTIC_TOKEN"
				cfg.Slack.MCP.ConnectorID = ""
				t.Setenv("SLACRAWL_MCP_DIAGNOSTIC_TOKEN", "synthetic-token")
				configPath := filepath.Join(dir, "config.toml")
				require.NoError(t, cfg.Save(configPath))
				var stdout, stderr bytes.Buffer
				app := &App{Stdout: &stdout, Stderr: &stderr}
				err = app.Run(ctx, []string{"--config", configPath, "sync", "--source", source, "--workspace", "TREQUEST", "--full"})
				if collision == "message" {
					require.ErrorContains(t, err, "does not match requested conversation")
				} else {
					require.ErrorContains(t, err, "workspace identity conflict")
				}
				output := fmt.Sprint(err) + stdout.String() + stderr.String()
				for _, value := range []string{foreignWorkspace, foreignChannel, foreignUser, "TREQUEST", canary} {
					require.NotContains(t, output, value)
				}
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				defer func() { require.NoError(t, st.Close()) }()
				for table, prior := range before {
					rows, err := st.QueryReadOnly(ctx, "select * from "+table)
					require.NoError(t, err)
					require.ElementsMatch(t, prior, rows, table)
				}
			})
		}
	}
}
