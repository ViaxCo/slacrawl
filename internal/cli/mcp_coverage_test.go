package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

// Keep this fixture on the public CLI/config/store APIs so it also exercises
// the unchanged parent: an incomplete response used to advance freshness.
func TestMCPNativeCoverageAcrossCLIPaths(t *testing.T) {
	const cursorCanary = "SYNTHETIC_MCP_COVERAGE_CURSOR"
	const incompleteError = "native MCP history or replies are incomplete; received messages were processed without advancing successful sync state; use --source api for paginated backfill"
	for _, source := range []string{"mcp", "connector"} {
		for _, policy := range []struct {
			name  string
			value *bool
		}{{"omitted", nil}, {"false", new(false)}, {"true", new(true)}} {
			for _, mode := range []string{"complete", "history-has-more", "replies-cursor"} {
				t.Run(source+"/"+policy.name+"/"+mode, func(t *testing.T) {
					var mu sync.Mutex
					var calls []mcpCoverageCall
					var serverErrors []string
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var req struct {
							ID     json.RawMessage `json:"id"`
							Method string          `json:"method"`
							Params struct {
								Name      string         `json:"name"`
								Arguments map[string]any `json:"arguments"`
							} `json:"params"`
						}
						fail := func(reason string) {
							mu.Lock()
							serverErrors = append(serverErrors, reason)
							mu.Unlock()
							http.Error(w, "synthetic fixture request rejected", http.StatusBadRequest)
						}
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							fail("invalid request JSON")
							return
						}
						var result any
						switch req.Method {
						case "initialize":
							result = map[string]any{"protocolVersion": "2025-03-26"}
						case "notifications/initialized":
							w.WriteHeader(http.StatusAccepted)
							return
						case "tools/list":
							result = map[string]any{"tools": []map[string]any{{"name": "slack_list_channels"}, {"name": "slack_get_users"}, {"name": "slack_get_channel_history"}, {"name": "slack_get_thread_replies"}}}
						case "tools/call":
							payload := map[string]any{"ok": true}
							switch req.Params.Name {
							case "slack_list_channels":
								payload["channels"] = []map[string]any{{"id": "CFIRST", "name": "first", "is_channel": true}, {"id": "CSECOND", "name": "second", "is_channel": true}}
							case "slack_get_users":
								payload["members"] = []map[string]any{{"id": "UONE", "name": "fixture"}}
							case "slack_get_channel_history", "slack_get_thread_replies":
								mu.Lock()
								calls = append(calls, mcpCoverageCall{req.Params.Name, req.Params.Arguments})
								mu.Unlock()
								channel, _ := req.Params.Arguments["channel_id"].(string)
								thread, _ := req.Params.Arguments["thread_ts"].(string)
								messages := mcpCoverageMessages(channel, thread)
								if messages == nil {
									fail("unexpected conversation or thread")
									return
								}
								payload["messages"] = messages
								if mode == "history-has-more" && channel == "CFIRST" && thread == "" {
									payload["has_more"] = true
								}
								if mode == "replies-cursor" && thread == "1710000000.000001" {
									payload["response_metadata"] = map[string]any{"next_cursor": cursorCanary}
								}
							default:
								fail("unexpected tool")
								return
							}
							raw, err := json.Marshal(payload)
							if err != nil {
								fail("invalid fixture response")
								return
							}
							result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
						default:
							fail("unexpected method")
							return
						}
						if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
							mu.Lock()
							serverErrors = append(serverErrors, "write response failed")
							mu.Unlock()
						}
					}))
					defer server.Close()
					dir := t.TempDir()
					cfg := config.Default()
					cfg.DBPath = filepath.Join(dir, "archive.db")
					cfg.CacheDir = filepath.Join(dir, "cache")
					cfg.LogDir = filepath.Join(dir, "logs")
					cfg.Share.RepoPath = filepath.Join(dir, "share")
					cfg.Share.Remote = ""
					cfg.Share.AutoUpdate = false
					cfg.Slack.Desktop.Path = filepath.Join(dir, "desktop")
					cfg.Slack.Desktop.Enabled = false
					cfg.Slack.Bot.Enabled, cfg.Slack.App.Enabled, cfg.Slack.User.Enabled = false, false, false
					cfg.Sync.IncludeDMs, cfg.Sync.FileMedia = policy.value, new(false)
					cfg.Slack.MCP.Enabled = true
					cfg.Slack.MCP.BaseURL = server.URL
					cfg.Slack.MCP.AuthPath = filepath.Join(dir, "auth.json")
					cfg.Slack.MCP.TokenEnv = "SLACRAWL_MCP_COVERAGE_TOKEN"
					cfg.Slack.MCP.AccountIDEnv = "SLACRAWL_MCP_COVERAGE_ACCOUNT"
					cfg.Slack.MCP.ConnectorID = ""
					t.Setenv("SLACRAWL_MCP_COVERAGE_TOKEN", "synthetic-token")
					t.Setenv("SLACRAWL_MCP_COVERAGE_ACCOUNT", "")
					t.Setenv("SLACRAWL_NO_UPDATE_CHECK", "1")
					path := filepath.Join(dir, "config.toml")
					require.NoError(t, cfg.Save(path))
					loaded, err := config.Load(path)
					require.NoError(t, err)
					require.Equal(t, policy.value, loaded.Sync.IncludeDMs)
					ctx := context.Background()
					st, err := store.Open(cfg.DBPath)
					require.NoError(t, err)
					require.NoError(t, st.EnsureWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: time.Unix(1, 0).UTC()}))
					// Seed both timestamps directly: SetSyncState stamps updated_at now.
					_, err = st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", "workspace", "TLOCAL", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
					require.NoError(t, err)
					before, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='mcp' and entity_type='workspace' and entity_id='TLOCAL'")
					require.NoError(t, err)
					require.NoError(t, st.Close())
					var stdout, stderr bytes.Buffer
					app := &App{Stdout: &stdout, Stderr: &stderr}
					runErr := app.Run(ctx, []string{"--config", path, "--no-color", "sync", "--source", source, "--workspace", "TLOCAL", "--full", "--with-media=false"})
					st, err = store.OpenReadOnly(cfg.DBPath)
					require.NoError(t, err)
					defer func() { require.NoError(t, st.Close()) }()
					after, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='mcp' and entity_type='workspace' and entity_id='TLOCAL'")
					require.NoError(t, err)
					rows, err := st.QueryReadOnly(ctx, "select channel_id || '|' || ts || '|' || text || '|' || source_name || '|' || source_rank as record from messages order by channel_id,ts")
					require.NoError(t, err)
					records := make([]string, 0, len(rows))
					for _, row := range rows {
						records = append(records, fmt.Sprint(row["record"]))
					}
					output := stdout.String() + stderr.String() + fmt.Sprint(runErr)
					counts := map[string]int{}
					cursorLeaked := strings.Contains(output, cursorCanary)
					for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state"} {
						rows, err := st.QueryReadOnly(ctx, "select * from "+table)
						require.NoError(t, err, table)
						counts[table] = len(rows)
						cursorLeaked = cursorLeaked || strings.Contains(fmt.Sprint(rows), cursorCanary)
					}
					ftsCounts := []int{}
					for _, token := range []string{"coveragealpha", "coveragebeta", "coveragegamma", "coveragedelta", "coverageepsilon"} {
						rows, err := st.QueryReadOnly(ctx, "select message_key from message_fts where message_fts match '"+token+"'")
						require.NoError(t, err)
						ftsCounts = append(ftsCounts, len(rows))
					}
					mu.Lock()
					observedCalls := append([]mcpCoverageCall(nil), calls...)
					observedServerErrors := append([]string(nil), serverErrors...)
					mu.Unlock()
					stateChanged := !reflect.DeepEqual(before, after)
					completed := strings.Contains(output, "Completed")
					// Capture every observation before the baseline's one policy failure.
					t.Logf("coverage observation: error=%t freshness_changed=%t completed=%t messages=%d events=%d heads=%d cursor_leaked=%t", runErr != nil, stateChanged, completed, len(rows), counts["message_events"], counts["message_event_heads"], cursorLeaked)
					require.Empty(t, observedServerErrors)
					require.Equal(t, []string{"CFIRST|1710000000.000001|coveragealpha|mcp|4", "CFIRST|1710000001.000002|coveragebeta|mcp|4", "CFIRST|1710000010.000003|coveragegamma|mcp|4", "CFIRST|1710000011.000004|coveragedelta|mcp|4", "CSECOND|1710000020.000005|coverageepsilon|mcp|4"}, records)
					require.Equal(t, 5, counts["message_events"])
					require.Equal(t, 5, counts["message_event_heads"])
					require.Equal(t, 5, counts["message_fts"])
					require.Equal(t, []int{1, 1, 1, 1, 1}, ftsCounts)
					require.False(t, cursorLeaked)
					require.Equal(t, []mcpCoverageCall{
						{"slack_get_channel_history", map[string]any{"channel_id": "CFIRST", "limit": float64(100)}},
						{"slack_get_thread_replies", map[string]any{"channel_id": "CFIRST", "thread_ts": "1710000000.000001"}},
						{"slack_get_thread_replies", map[string]any{"channel_id": "CFIRST", "thread_ts": "1710000010.000003"}},
						{"slack_get_channel_history", map[string]any{"channel_id": "CSECOND", "limit": float64(100)}},
					}, observedCalls)
					if mode == "complete" {
						require.NoError(t, runErr)
						require.True(t, stateChanged)
						require.True(t, completed)
					} else if runErr == nil || runErr.Error() != incompleteError || stateChanged || completed {
						t.Fatalf("coverage boundary: error=%t expected_error=%t freshness_changed=%t completed=%t messages=%d", runErr != nil, runErr != nil && runErr.Error() == incompleteError, stateChanged, completed, len(records))
					}
				})
			}
		}
	}
}

type mcpCoverageCall struct {
	Name string
	Args map[string]any
}

func mcpCoverageMessages(channel, thread string) []map[string]any {
	message := func(ts, text string) map[string]any {
		return map[string]any{"channel": channel, "context_team_id": "TLOCAL", "ts": ts, "user": "UONE", "text": text}
	}
	if channel == "CSECOND" && thread == "" {
		return []map[string]any{message("1710000020.000005", "coverageepsilon")}
	}
	if channel != "CFIRST" {
		return nil
	}
	root1, root2 := message("1710000000.000001", "coveragealpha"), message("1710000010.000003", "coveragegamma")
	root1["reply_count"], root1["latest_reply"] = 1, "1710000001.000002"
	root2["reply_count"], root2["latest_reply"] = 1, "1710000011.000004"
	// History and replies use identical parent projections, avoiding an extra
	// legitimate parent event when the thread response refreshes metadata.
	switch thread {
	case "":
		return []map[string]any{root1, root2}
	case "1710000000.000001":
		reply := message("1710000001.000002", "coveragebeta")
		reply["thread_ts"] = thread
		return []map[string]any{root1, reply}
	case "1710000010.000003":
		reply := message("1710000011.000004", "coveragedelta")
		reply["thread_ts"] = thread
		return []map[string]any{root2, reply}
	}
	return nil
}
