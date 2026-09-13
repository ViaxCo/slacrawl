package slackmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const coverageCursorCanary = "SYNTHETIC_NATIVE_COVERAGE_CURSOR"
const coverageMoreError = "native MCP history or replies are incomplete; received messages were processed without advancing successful sync state; use --source api for paginated backfill"
const coverageLimitedError = "native MCP coverage is incomplete because of Slack history/message limits; received messages were processed without advancing successful sync state; review Slack workspace history availability"

func TestMCPNativeCoverageSignalsAndEarlyReturns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		thread   bool
		signal   string
		shape    string
		noThread bool
		wantRows []string
		wantErr  string
	}{
		{"history-limited", false, "limited", "", false, []string{"alpha", "beta", "gamma", "delta", "epsilon"}, coverageLimitedError},
		{"history-cursor", false, "cursor", "", false, []string{"alpha", "beta", "gamma", "delta", "epsilon"}, coverageMoreError},
		{"replies-has-more", true, "more", "", false, []string{"alpha", "beta", "gamma", "delta", "epsilon"}, coverageMoreError},
		{"replies-limited", true, "limited", "", false, []string{"alpha", "beta", "gamma", "delta", "epsilon"}, coverageLimitedError},
		{"filtered-empty-history", false, "more", "filtered", false, []string{"epsilon"}, coverageMoreError},
		{"parent-only-replies", true, "more", "parent-only", false, []string{"alpha", "gamma", "delta", "epsilon"}, coverageMoreError},
		{"empty-replies", true, "more", "empty", false, []string{"alpha", "gamma", "delta", "epsilon"}, coverageMoreError},
		{"missing-thread-tool", false, "more", "", true, []string{"alpha", "gamma", "epsilon"}, coverageMoreError},
		{"false-flags-whitespace-cursor", false, "none", "", true, []string{"alpha", "gamma", "epsilon"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := newCoverageGateway(t, !tc.noThread, func(call coverageCall, payload map[string]any) int {
				if call.Channel == "CFIRST" && ((!tc.thread && call.Thread == "") || (tc.thread && call.Thread == "1710000000.000001")) {
					switch tc.signal {
					case "limited":
						payload["is_limited"] = true
					case "cursor":
						payload["response_metadata"] = map[string]any{"next_cursor": coverageCursorCanary}
					case "more":
						payload["has_more"] = true
					case "none":
						payload["has_more"], payload["is_limited"] = false, false
						payload["response_metadata"] = map[string]any{"next_cursor": " \t\n"}
					}
					if tc.shape == "parent-only" {
						payload["messages"] = payload["messages"].([]map[string]any)[:1]
					} else if tc.shape == "empty" {
						payload["messages"] = []map[string]any{}
					}
				}
				return http.StatusOK
			})
			st, before := coverageStore(t)
			opts := coverageOptions(t, gateway, admission.Default)
			if tc.shape == "filtered" {
				opts.Since = "1710000015"
			}
			summary, err := Sync(context.Background(), st, opts)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.NotEqual(t, before, coverageFreshness(t, st))
			} else {
				require.EqualError(t, err, tc.wantErr)
				require.Equal(t, before, coverageFreshness(t, st))
			}
			assertCoverageRows(t, st, tc.wantRows)
			require.Equal(t, len(tc.wantRows), summary.Messages+summary.Replies)
			require.Equal(t, 2, summary.Channels)
			calls := gateway.dataCalls(t)
			wantCalls := []coverageCall{{"CFIRST", ""}, {"CFIRST", "1710000000.000001"}, {"CFIRST", "1710000010.000003"}, {"CSECOND", ""}}
			if tc.noThread || tc.shape == "filtered" {
				wantCalls = []coverageCall{{"CFIRST", ""}, {"CSECOND", ""}}
			}
			require.Equal(t, wantCalls, calls, "later successes must not erase earlier incomplete evidence")
			require.NotContains(t, fmt.Sprint(err), coverageCursorCanary)
			for table, rows := range admissionTableSnapshot(t, st) {
				require.NotContains(t, fmt.Sprint(rows), coverageCursorCanary, table)
			}
		})
	}
}

func TestMCPNativeCoverageRequiresSuccessfulResponses(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Exclude, admission.Include} {
		for _, response := range []string{"history", "replies"} {
			for _, success := range []string{"missing", "false"} {
				t.Run(fmt.Sprintf("policy=%d/%s/%s", policy, response, success), func(t *testing.T) {
					gateway := newCoverageGateway(t, true, func(call coverageCall, payload map[string]any) int {
						if call.Channel == "CFIRST" && (response == "history" && call.Thread == "" || response == "replies" && call.Thread == "1710000000.000001") {
							delete(payload, "ok")
							if success == "false" {
								payload["ok"] = false
							}
						}
						return http.StatusOK
					})
					st, before := coverageStore(t)
					_, err := Sync(context.Background(), st, coverageOptions(t, gateway, policy))
					wantError := "read MCP channel: native MCP history did not report successful Slack response"
					wantRows := []string{}
					wantCalls := []coverageCall{{"CFIRST", ""}}
					if response == "replies" {
						wantError = "read MCP thread: native MCP replies did not report successful Slack response"
						wantRows = []string{"alpha", "gamma"}
						wantCalls = append(wantCalls, coverageCall{"CFIRST", "1710000000.000001"})
					}
					require.EqualError(t, err, wantError)
					require.Equal(t, before, coverageFreshness(t, st))
					assertCoverageRows(t, st, wantRows)
					require.Equal(t, wantCalls, gateway.dataCalls(t))
				})
			}
		}
	}
}

func TestMCPNativeCoverageConcreteErrorsWin(t *testing.T) {
	for _, kind := range []string{"later-http", "later-identity", "later-store", "invalid-old-message"} {
		t.Run(kind, func(t *testing.T) {
			gateway := newCoverageGateway(t, true, func(call coverageCall, payload map[string]any) int {
				if call.Channel == "CFIRST" && call.Thread == "" {
					payload["has_more"] = true
					if kind == "invalid-old-message" {
						payload["messages"].([]map[string]any)[0]["context_team_id"] = "TFOREIGN"
					}
				}
				if call.Channel == "CSECOND" {
					if kind == "later-http" {
						return http.StatusServiceUnavailable
					}
					if kind == "later-identity" {
						payload["messages"].([]map[string]any)[0]["context_team_id"] = "TFOREIGN"
					}
				}
				return http.StatusOK
			})
			st, before := coverageStore(t)
			var foreignBefore []map[string]any
			if kind == "later-store" {
				require.NoError(t, st.EnsureWorkspace(context.Background(), store.Workspace{ID: "TFOREIGN", Name: "foreign", RawJSON: "{}", UpdatedAt: time.Unix(1, 0).UTC()}))
				require.NoError(t, st.EnsureChannel(context.Background(), store.Channel{ID: "CSECOND", WorkspaceID: "TFOREIGN", Name: "foreign", Kind: "public_channel", RawJSON: "{}", UpdatedAt: time.Unix(1, 0).UTC()}))
				var err error
				foreignBefore, err = st.QueryReadOnly(context.Background(), "select * from channels where id='CSECOND'")
				require.NoError(t, err)
			}
			opts := coverageOptions(t, gateway, admission.Default)
			if kind == "invalid-old-message" {
				opts.Since = "1710000015"
			}
			_, err := Sync(context.Background(), st, opts)
			wantError := "read MCP channel: MCP message context workspace does not match the configured workspace"
			if kind == "later-http" {
				wantError = "read MCP channel: MCP tools/call returned HTTP 503"
			} else if kind == "later-store" {
				wantError = "store MCP data: workspace identity conflict; check the configured workspace and archive"
			}
			require.EqualError(t, err, wantError)
			require.Equal(t, before, coverageFreshness(t, st))
			wantRows := []string{"alpha", "beta", "gamma", "delta"}
			wantCalls := []coverageCall{{"CFIRST", ""}, {"CFIRST", "1710000000.000001"}, {"CFIRST", "1710000010.000003"}, {"CSECOND", ""}}
			if kind == "invalid-old-message" {
				wantRows = []string{}
				wantCalls = wantCalls[:1]
			}
			assertCoverageRows(t, st, wantRows)
			require.Equal(t, wantCalls, gateway.dataCalls(t))
			if kind == "later-store" {
				foreignAfter, queryErr := st.QueryReadOnly(context.Background(), "select * from channels where id='CSECOND'")
				require.NoError(t, queryErr)
				require.Equal(t, foreignBefore, foreignAfter)
			}
		})
	}
}

func TestMCPNativeCoverageRejectsWrongFlagTypes(t *testing.T) {
	for _, field := range []string{"has_more", "is_limited", "next_cursor"} {
		t.Run(field, func(t *testing.T) {
			payload := map[string]any{"ok": true, field: "true"}
			if field == "next_cursor" {
				delete(payload, field)
				payload["response_metadata"] = map[string]any{field: true}
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			var response referenceResponse
			require.EqualError(t, decodeReferenceResponse(string(raw), &response), "invalid Slack API response")
		})
	}
}

type coverageCall struct {
	Channel string
	Thread  string
}

type coverageGateway struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []coverageCall
	errors []string
}

func newCoverageGateway(t *testing.T, threads bool, modify func(coverageCall, map[string]any) int) *coverageGateway {
	t.Helper()
	g := &coverageGateway{}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		fail := func(reason string) {
			g.mu.Lock()
			g.errors = append(g.errors, reason)
			g.mu.Unlock()
			http.Error(w, "synthetic request rejected", http.StatusBadRequest)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fail("invalid JSON")
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
			tools := []map[string]any{{"name": "slack_list_channels"}, {"name": "slack_get_users"}, {"name": "slack_get_channel_history"}}
			if threads {
				tools = append(tools, map[string]any{"name": "slack_get_thread_replies"})
			}
			result = map[string]any{"tools": tools}
		case "tools/call":
			payload := map[string]any{"ok": true}
			switch req.Params.Name {
			case "slack_list_channels":
				payload["channels"] = []map[string]any{{"id": "CFIRST", "name": "first", "is_channel": true}, {"id": "CSECOND", "name": "second", "is_channel": true}}
			case "slack_get_users":
				payload["members"] = []any{}
			case "slack_get_channel_history", "slack_get_thread_replies":
				channel, _ := req.Params.Arguments["channel_id"].(string)
				thread, _ := req.Params.Arguments["thread_ts"].(string)
				call := coverageCall{channel, thread}
				g.mu.Lock()
				g.calls = append(g.calls, call)
				g.mu.Unlock()
				messages := coverageMessages(channel, thread)
				if messages == nil {
					fail("unexpected conversation")
					return
				}
				payload["messages"] = messages
				if status := modify(call, payload); status != http.StatusOK {
					http.Error(w, coverageCursorCanary, status)
					return
				}
			default:
				fail("unexpected tool")
				return
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				fail("invalid payload")
				return
			}
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
		default:
			fail("unexpected method")
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
			g.mu.Lock()
			g.errors = append(g.errors, "write response failed")
			g.mu.Unlock()
		}
	}))
	t.Cleanup(g.server.Close)
	return g
}

func (g *coverageGateway) dataCalls(t *testing.T) []coverageCall {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	require.Empty(t, g.errors)
	return append([]coverageCall(nil), g.calls...)
}

func coverageOptions(t *testing.T, gateway *coverageGateway, policy admission.DMPolicy) Options {
	t.Helper()
	t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
	t.Setenv("TEST_MCP_ACCOUNT", "")
	cfg := testMCPConfig(gateway.server.URL)
	cfg.ConnectorID = ""
	cfg.AuthPath = filepath.Join(t.TempDir(), "auth.json")
	return Options{WorkspaceID: "TLOCAL", Full: true, Config: cfg, DMPolicy: policy}
}

func coverageStore(t *testing.T) (*store.Store, []map[string]any) {
	t.Helper()
	st := admissionStore(t)
	ctx := context.Background()
	require.NoError(t, st.EnsureWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: time.Unix(1, 0).UTC()}))
	_, err := st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", "workspace", "TLOCAL", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
	require.NoError(t, err)
	return st, coverageFreshness(t, st)
}

func coverageFreshness(t *testing.T, st *store.Store) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), "select * from sync_state where source_name='mcp' and entity_type='workspace' and entity_id='TLOCAL'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows
}

func assertCoverageRows(t *testing.T, st *store.Store, texts []string) {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), "select text,source_name,source_rank from messages order by channel_id,ts")
	require.NoError(t, err)
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, fmt.Sprint(row["text"]))
		require.Equal(t, "mcp", row["source_name"])
		require.EqualValues(t, 4, row["source_rank"])
	}
	require.Equal(t, texts, got)
	for _, table := range []string{"message_events", "message_event_heads", "message_fts"} {
		// Event heads use a composite primary key WITHOUT ROWID at runtime.
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err, table)
		require.Len(t, rows, len(texts), table)
		require.NotContains(t, fmt.Sprint(rows), coverageCursorCanary)
	}
}

func coverageMessages(channel, thread string) []map[string]any {
	message := func(ts, text string) map[string]any {
		return map[string]any{"channel": channel, "context_team_id": "TLOCAL", "ts": ts, "user": "UONE", "text": text}
	}
	if channel == "CSECOND" && thread == "" {
		return []map[string]any{message("1710000020.000005", "epsilon")}
	}
	if channel != "CFIRST" {
		return nil
	}
	root1, root2 := message("1710000000.000001", "alpha"), message("1710000010.000003", "gamma")
	root1["reply_count"], root1["latest_reply"] = 1, "1710000001.000002"
	root2["reply_count"], root2["latest_reply"] = 1, "1710000011.000004"
	switch thread {
	case "":
		return []map[string]any{root1, root2}
	case "1710000000.000001", "1710000010.000003":
		parent, ts, text := root1, "1710000001.000002", "beta"
		if strings.HasSuffix(thread, "000003") {
			parent, ts, text = root2, "1710000011.000004", "delta"
		}
		reply := message(ts, text)
		reply["thread_ts"] = thread
		return []map[string]any{parent, reply}
	}
	return nil
}
