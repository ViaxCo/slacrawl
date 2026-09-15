package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/store"
)

const (
	mcpWorkRoot         = "1710000001.000000"
	mcpWorkChild        = "1710000002.000000"
	mcpWorkNewRoot      = "1710001001.000000"
	mcpWorkNewChild     = "1710001002.000000"
	mcpWorkSince        = "1710000500.000000"
	mcpWorkMoreError    = "native MCP history or replies are incomplete; received messages were processed without advancing successful sync state; use --source api for paginated backfill"
	mcpWorkLimitedError = "native MCP coverage is incomplete because of Slack history/message limits; received messages were processed without advancing successful sync state; review Slack workspace history availability"
	mcpWorkToolError    = "MCP replies work is pending but the connector does not provide a read-thread tool; configure a connector with thread support and retry"
)

// Keep the entire fixture parent-compatible. In restart cases both invocations
// finish and are observed before the boundary, so a correct first error cannot
// conceal the parent's later false success after losing the only reply hint.
func TestMCPRetainedThreadsFromCLI(t *testing.T) {
	for _, source := range []string{"mcp", "connector"} {
		for _, mode := range []string{"retained", "native-cursor", "native-limited", "since", "full-since", "no-tool-pending", "no-tool-fresh", "text-restart", "text-renewal"} {
			if mode == "text-renewal" && source == "connector" {
				continue
			}
			t.Run(source+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				cfg, path := userPrimaryConfig(t)
				cfg.Slack.Bot.Enabled, cfg.Slack.User.Enabled, cfg.Slack.App.Enabled = false, false, false
				cfg.Sync.IncludeDMs = new(false)
				textAdapter := strings.HasPrefix(mode, "text-")
				if textAdapter {
					cfg.Sync.IncludeDMs = nil
					if source == "connector" {
						cfg.Sync.IncludeDMs = new(true)
					}
				}
				fixture := &mcpWorkFixture{mode: mode, text: textAdapter, dbPath: cfg.DBPath, calls: []mcpCoverageCall{}, errors: []string{}}
				server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
				defer server.Close()
				cfg.Slack.MCP.Enabled, cfg.Slack.MCP.Transport, cfg.Slack.MCP.BaseURL = true, "http", server.URL
				cfg.Slack.MCP.TokenEnv, cfg.Slack.MCP.AccountIDEnv = "SLACRAWL_THREAD_WORK_TOKEN", "SLACRAWL_THREAD_WORK_ACCOUNT"
				cfg.Slack.MCP.ConnectorID = ""
				cfg.Slack.MCP.PageSize, cfg.Slack.MCP.SearchLimit = 100, 20
				t.Setenv(cfg.Slack.MCP.TokenEnv, "synthetic-thread-token")
				t.Setenv(cfg.Slack.MCP.AccountIDEnv, "")
				require.NoError(t, cfg.Save(path))
				configBefore, err := os.ReadFile(path)
				require.NoError(t, err)
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Unix(1700000000, 0).UTC()
				require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
				require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
				// Rank 4 and no archived child are essential: a richer source or
				// retained child would prevent the parent from losing this hint.
				if mode != "no-tool-fresh" {
					hints := 1
					if mode == "native-cursor" || mode == "native-limited" || mode == "text-restart" {
						hints = 0 // These jobs must come from the duplicate history page.
					}
					require.NoError(t, st.UpsertMessage(ctx, store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: mcpWorkRoot, Text: "mcproot", NormalizedText: "mcproot", ReplyCount: hints, SourceName: "mcp", SourceRank: 4, RawJSON: "{}", UpdatedAt: now}, nil))
				}
				_, err = st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", "workspace", "TLOCAL", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
				require.NoError(t, err)
				sliced := mode == "since" || mode == "full-since"
				if sliced || mode == "no-tool-pending" || mode == "text-renewal" {
					_, err = st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", "thread_pending_v1", retainedCLIKey("TLOCAL", "C123", mcpWorkRoot), "seed-generation", "2020-01-01T00:00:00Z")
					require.NoError(t, err)
				}
				beforePending := mcpWorkPending(t, st)
				beforeWorkspace := mcpWorkWorkspace(t, st)
				beforeRoot := apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkRoot+"'")
				require.NoError(t, st.Close())

				args := []string{"--config", path, "--json", "sync", "--source", source, "--workspace", "TLOCAL", "--channels", "C123", "--with-media=false"}
				if sliced {
					args = append(args, "--since", mcpWorkSince)
				}
				if mode == "full-since" {
					args = append(args, "--full")
				}
				attempts := 1
				if mode == "native-cursor" || mode == "native-limited" || mode == "text-restart" {
					attempts = 2
				}
				observations := make([]mcpWorkObservation, 0, attempts)
				for attempt := 0; attempt < attempts; attempt++ {
					fixture.mu.Lock()
					fixture.attempt, fixture.calls = attempt, []mcpCoverageCall{}
					fixture.mu.Unlock()
					var stdout, stderr bytes.Buffer
					app := &App{Stdout: &stdout, Stderr: &stderr}
					runErr := app.Run(ctx, args)
					st, err = store.OpenReadOnly(cfg.DBPath)
					require.NoError(t, err)
					observation := mcpWorkObservation{
						Error: userPrimaryError(runErr), Completion: stdout.Len() != 0,
						Rows:    apiKeyRows(t, st, "select ts,coalesce(thread_ts,'') as thread_ts,text,reply_count,source_name,source_rank from messages order by ts"),
						Pending: mcpWorkPending(t, st), Workspace: mcpWorkWorkspace(t, st),
						Counts: apiCompletenessCounts(t, st),
						FTS:    apiKeyRows(t, st, "select message_key from message_fts order by message_key"),
					}
					observation.WorkspacePreserved = reflect.DeepEqual(beforeWorkspace, observation.Workspace)
					observation.PendingPreserved = reflect.DeepEqual(beforePending, observation.Pending)
					fixture.mu.Lock()
					observation.Calls = append([]mcpCoverageCall{}, fixture.calls...)
					observation.ServerErrors = append([]string{}, fixture.errors...)
					fixture.mu.Unlock()
					require.NoError(t, st.Close())
					observations = append(observations, observation)
				}
				configAfter, err := os.ReadFile(path)
				require.NoError(t, err)
				body, err := json.Marshal(map[string]any{"attempts": observations, "config_preserved": bytes.Equal(configBefore, configAfter)})
				require.NoError(t, err)
				t.Logf("MCP retained thread observation: %s", body)

				last := observations[len(observations)-1]
				wantError, wantPending, wantChildren := "", 0, 1
				switch mode {
				case "since", "full-since":
					wantPending = 1
				case "no-tool-pending":
					wantError, wantPending, wantChildren = mcpWorkToolError, 1, 0
				case "no-tool-fresh":
					wantChildren = 0
				case "text-renewal":
					wantPending, wantChildren = 1, 0
				}
				children := 0
				for _, row := range last.Rows {
					if row["thread_ts"] != "" {
						children++
					}
				}
				boundary := last.Error == wantError && len(last.Pending) == wantPending && children == wantChildren
				if sliced {
					boundary = boundary && len(mcpWorkReplyCalls(last.Calls)) == 1 && mcpWorkReplyCalls(last.Calls)[0].Args["thread_ts"] == mcpWorkNewRoot
				}
				if attempts == 2 {
					boundary = boundary && len(observations[0].Pending) == 1
				}
				if mode == "text-renewal" {
					boundary = boundary && len(mcpWorkReplyCalls(last.Calls)) == 1
				}
				if !boundary {
					t.Fatalf("MCP retained thread boundary: error=%q attempts=%d replies=%d pending=%d children=%d", last.Error, attempts, len(mcpWorkReplyCalls(last.Calls)), len(last.Pending), children)
				}
				require.Equal(t, configBefore, configAfter)
				for attempt, observed := range observations {
					require.Empty(t, observed.ServerErrors)
					want := ""
					if attempt == 0 {
						switch mode {
						case "native-cursor":
							want = mcpWorkMoreError
						case "native-limited":
							want = mcpWorkLimitedError
						case "text-restart":
							want = "read MCP thread: MCP tools/call returned HTTP 503"
						case "no-tool-pending":
							want = mcpWorkToolError
						}
					}
					require.Equal(t, want, observed.Error)
					require.Equal(t, want == "", observed.Completion)
					require.Equal(t, want != "", observed.WorkspacePreserved)
					require.Equal(t, mcpWorkExpectedCalls(mode, textAdapter, attempt), observed.Calls)
					require.Len(t, observed.FTS, len(observed.Rows))
					require.Equal(t, len(observed.Rows), observed.Counts["message_event_heads"])
					for _, row := range observed.Rows {
						require.Equal(t, "mcp", row["source_name"])
						require.Equal(t, int64(4), row["source_rank"])
					}
				}
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				defer func() { require.NoError(t, st.Close()) }()
				if sliced || mode == "no-tool-pending" {
					require.Equal(t, beforePending, last.Pending)
				}
				if sliced {
					require.Equal(t, beforeRoot, apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkRoot+"'"))
					require.Empty(t, apiKeyRows(t, st, "select * from message_fts where message_fts match 'mcpoldreplycanary'"))
				}
				if mode == "text-renewal" {
					require.Equal(t, "renewed-generation", last.Pending[0]["value"])
					require.Equal(t, "2021-01-01T00:00:00Z", last.Pending[0]["updated_at"])
				}
				if wantChildren == 1 {
					root, child, token, mention := mcpWorkRoot, mcpWorkChild, "mcpchild", "UCHILD"
					if sliced {
						root, child, token, mention = mcpWorkNewRoot, mcpWorkNewChild, "mcpnewreply", "UNEW"
					}
					require.Equal(t, []map[string]any{{"workspace_id": "TLOCAL", "channel_id": "C123", "ts": child, "thread_ts": root, "source_name": "mcp", "source_rank": int64(4)}}, apiKeyRows(t, st, "select workspace_id,channel_id,ts,thread_ts,source_name,source_rank from messages where ts='"+child+"'"))
					require.Len(t, apiKeyRows(t, st, "select message_key from message_fts where message_fts match '"+token+"'"), 1)
					require.Equal(t, []map[string]any{{"target_id": mention}}, apiKeyRows(t, st, "select target_id from message_mentions where ts='"+child+"'"))
					raw := apiKeyRows(t, st, "select raw_json from messages where ts='"+child+"'")
					var retained map[string]any
					require.NoError(t, json.Unmarshal([]byte(raw[0]["raw_json"].(string)), &retained))
					require.Equal(t, child, retained["ts"])
					require.Equal(t, root, retained["thread_ts"])
					require.Equal(t, "C123", retained["channel_id"])
				}
			})
		}
	}
}

type mcpWorkObservation struct {
	Error              string            `json:"error"`
	Completion         bool              `json:"completion"`
	Calls              []mcpCoverageCall `json:"calls"`
	Rows               []map[string]any  `json:"rows"`
	Pending            []map[string]any  `json:"pending"`
	Workspace          []map[string]any  `json:"workspace"`
	Counts             map[string]int    `json:"counts"`
	FTS                []map[string]any  `json:"fts"`
	WorkspacePreserved bool              `json:"workspace_preserved"`
	PendingPreserved   bool              `json:"pending_preserved"`
	ServerErrors       []string          `json:"server_errors"`
}

func mcpWorkPending(t *testing.T, st *store.Store) []map[string]any {
	t.Helper()
	return apiKeyRows(t, st, "select * from sync_state where source_name='mcp' and entity_type='thread_pending_v1' order by entity_id")
}

func mcpWorkWorkspace(t *testing.T, st *store.Store) []map[string]any {
	t.Helper()
	return apiKeyRows(t, st, "select * from sync_state where source_name='mcp' and entity_type='workspace' order by entity_id")
}

func mcpWorkReplyCalls(calls []mcpCoverageCall) []mcpCoverageCall {
	out := []mcpCoverageCall{}
	for _, call := range calls {
		if call.Name == "slack_get_thread_replies" || call.Name == "slack_read_thread" {
			out = append(out, call)
		}
	}
	return out
}

func mcpWorkExpectedCalls(mode string, text bool, attempt int) []mcpCoverageCall {
	if text {
		history := map[string]any{"channel_id": "C123", "limit": float64(100), "response_format": "detailed"}
		if attempt > 0 {
			history["oldest"] = "1709996401.000000"
		}
		calls := []mcpCoverageCall{{"slack_read_channel", history}, {"slack_read_thread", map[string]any{"channel_id": "C123", "message_ts": mcpWorkRoot, "limit": float64(100), "response_format": "detailed"}}}
		if mode == "text-restart" && attempt == 0 {
			calls = append(calls, mcpCoverageCall{"slack_read_thread", map[string]any{"channel_id": "C123", "message_ts": mcpWorkRoot, "cursor": "thread-next", "limit": float64(100), "response_format": "detailed"}})
		}
		return calls
	}
	calls := []mcpCoverageCall{{"slack_list_channels", map[string]any{"limit": float64(20)}}, {"slack_get_channel_history", map[string]any{"channel_id": "C123", "limit": float64(100)}}}
	if !strings.HasPrefix(mode, "no-tool-") {
		root := mcpWorkRoot
		if mode == "since" || mode == "full-since" {
			root = mcpWorkNewRoot
		}
		calls = append(calls, mcpCoverageCall{"slack_get_thread_replies", map[string]any{"channel_id": "C123", "thread_ts": root}})
	}
	return calls
}

type mcpWorkFixture struct {
	mu           sync.Mutex
	mode, dbPath string
	text         bool
	attempt      int
	calls        []mcpCoverageCall
	errors       []string
}

func (f *mcpWorkFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	fail := func(reason string) {
		f.mu.Lock()
		f.errors = append(f.errors, reason)
		f.mu.Unlock()
		http.Error(w, "synthetic fixture rejected request", http.StatusBadRequest)
	}
	if r.Header.Get("Authorization") != "Bearer synthetic-thread-token" {
		fail("unexpected credential")
		return
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
		names := []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history"}
		if !strings.HasPrefix(f.mode, "no-tool-") {
			names = append(names, "slack_get_thread_replies")
		}
		if f.text {
			names = []string{"slack_search_channels", "slack_search_users", "slack_read_channel", "slack_read_thread"}
		}
		tools := []map[string]any{}
		for _, name := range names {
			tools = append(tools, map[string]any{"name": name})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		f.mu.Lock()
		f.calls = append(f.calls, mcpCoverageCall{req.Params.Name, req.Params.Arguments})
		attempt := f.attempt
		f.mu.Unlock()
		payload := map[string]any{"ok": true}
		switch req.Params.Name {
		case "slack_list_channels":
			payload["channels"] = []map[string]any{{"id": "C123", "name": "fixture", "is_channel": true}}
		case "slack_get_channel_history", "slack_read_channel":
			messages := []map[string]any{mcpWorkMessage(mcpWorkRoot, "", false)}
			if f.mode == "since" || f.mode == "full-since" {
				messages = []map[string]any{mcpWorkMessage(mcpWorkNewRoot, "", true)}
			} else if f.mode == "no-tool-fresh" || f.mode == "text-renewal" {
				messages = []map[string]any{mcpWorkMessage(mcpWorkRoot, "", true)}
			} else if attempt == 0 && (f.mode == "native-cursor" || f.mode == "native-limited" || f.mode == "text-restart") {
				messages = append([]map[string]any{mcpWorkMessage(mcpWorkRoot, "", true)}, messages...)
			}
			payload["messages"] = messages
			if f.text {
				payload = map[string]any{"messages": mcpWorkTextHistory(messages)}
			}
		case "slack_get_thread_replies", "slack_read_thread":
			root, _ := req.Params.Arguments["thread_ts"].(string)
			if f.text {
				root, _ = req.Params.Arguments["message_ts"].(string)
			}
			if root != mcpWorkRoot && root != mcpWorkNewRoot {
				fail("unexpected thread root")
				return
			}
			cursor, _ := req.Params.Arguments["cursor"].(string)
			if f.mode == "text-restart" && attempt == 0 && cursor == "thread-next" {
				http.Error(w, "synthetic page failure", http.StatusServiceUnavailable)
				return
			}
			child := mcpWorkChild
			if root == mcpWorkNewRoot {
				child = mcpWorkNewChild
			}
			messages := []map[string]any{mcpWorkMessage(root, "", false), mcpWorkMessage(child, root, false)}
			if root == mcpWorkRoot && (f.mode == "since" || f.mode == "full-since") {
				messages[1]["text"] = "mcpoldreplycanary <@UOLD>"
			}
			if attempt == 0 && f.mode == "native-cursor" {
				messages = []map[string]any{}
				payload["response_metadata"] = map[string]any{"next_cursor": "native-next"}
			}
			if attempt == 0 && f.mode == "native-limited" {
				messages = messages[:1]
				payload["is_limited"] = true
			}
			payload["messages"] = messages
			if f.text {
				payload = map[string]any{"messages": mcpWorkTextThread(root, child)}
				if cursor == "" && attempt == 0 {
					payload["pagination_info"] = "next cursor `thread-next`"
				}
			}
			if f.mode == "text-renewal" && cursor == "" {
				st, err := store.Open(f.dbPath)
				if err == nil {
					_, err = st.DB().ExecContext(r.Context(), "update sync_state set value=?,updated_at=? where source_name='mcp' and entity_type='thread_pending_v1' and entity_id=?", "renewed-generation", "2021-01-01T00:00:00Z", retainedCLIKey("TLOCAL", "C123", root))
					closeErr := st.Close()
					if err == nil {
						err = closeErr
					}
				}
				if err != nil {
					fail("renew pending work failed")
					return
				}
			}
		default:
			fail("unexpected tool")
			return
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			fail("marshal response failed")
			return
		}
		result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
	default:
		fail("unexpected method")
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
		f.mu.Lock()
		f.errors = append(f.errors, "write response failed")
		f.mu.Unlock()
	}
}

func mcpWorkMessage(ts, thread string, hint bool) map[string]any {
	text := "mcproot"
	switch ts {
	case mcpWorkChild:
		text = "mcpchild <@UCHILD>"
	case mcpWorkNewRoot:
		text = "mcpnewroot"
	case mcpWorkNewChild:
		text = "mcpnewreply <@UNEW>"
	}
	msg := map[string]any{"channel": "C123", "context_team_id": "TLOCAL", "ts": ts, "thread_ts": thread, "user": "UONE", "text": text}
	if hint {
		msg["reply_count"] = 1
	}
	return msg
}

func mcpWorkTextHistory(messages []map[string]any) string {
	var text strings.Builder
	text.WriteString("Channel: #fixture (C123)")
	for _, message := range messages {
		fmt.Fprintf(&text, "\n\n=== Message from Fixture (UONE) at 2024-03-09T16:00:00Z === \nMessage TS: %s\n%s", message["ts"], message["text"])
		if message["reply_count"] == 1 {
			text.WriteString("\nThread: 1 replies (latest: " + mcpWorkChild + ")")
		}
	}
	return text.String()
}

func mcpWorkTextThread(root, child string) string {
	return fmt.Sprintf("From: Fixture (UONE)\nTime: 2024-03-09T16:00:00Z\nMessage TS: %s\nmcproot\n\n=== THREAD REPLIES ===\n\n--- Reply 1 ---\nFrom: Fixture (UONE)\nTime: 2024-03-09T16:00:01Z\nMessage TS: %s\nmcpchild <@UCHILD>", root, child)
}
