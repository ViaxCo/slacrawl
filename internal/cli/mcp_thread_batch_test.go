package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/store"
)

func TestMCPHistoryBatchKeepsChildWorkFromCLI(t *testing.T) {
	for _, source := range []string{"mcp", "connector"} {
		for _, adapter := range []string{"native", "text"} {
			t.Run(source+"/"+adapter, func(t *testing.T) {
				ctx := context.Background()
				cfg, path := userPrimaryConfig(t)
				cfg.Slack.Bot.Enabled, cfg.Slack.User.Enabled, cfg.Slack.App.Enabled = false, false, false
				cfg.Sync.IncludeDMs = new(false)
				text := adapter == "text"
				if text {
					cfg.Sync.IncludeDMs = nil
				}
				fixture := &mcpBatchFixture{text: text, calls: []mcpCoverageCall{}, errors: []string{}}
				server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
				defer server.Close()
				cfg.Slack.MCP.Enabled, cfg.Slack.MCP.Transport, cfg.Slack.MCP.BaseURL = true, "http", server.URL
				cfg.Slack.MCP.TokenEnv, cfg.Slack.MCP.AccountIDEnv = "SLACRAWL_MCP_BATCH_TOKEN", "SLACRAWL_MCP_BATCH_ACCOUNT"
				cfg.Slack.MCP.ConnectorID = ""
				// The server honors an explicitly requested page larger than the
				// store batch. Native history forwards this limit without a cap.
				cfg.Slack.MCP.PageSize, cfg.Slack.MCP.SearchLimit, cfg.Slack.MCP.MaxPages = 501, 20, 1
				t.Setenv(cfg.Slack.MCP.TokenEnv, "synthetic-batch-token")
				t.Setenv(cfg.Slack.MCP.AccountIDEnv, "")
				require.NoError(t, cfg.Save(path))
				configBefore, err := os.ReadFile(path)
				require.NoError(t, err)
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Unix(1700000000, 0).UTC()
				require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
				require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
				if text {
					// Text history has no ThreadTS. An arriving root can instead
					// make a previously archived orphan child eligible evidence.
					require.NoError(t, st.UpsertMessage(ctx, store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: mcpWorkChild, ThreadTS: mcpWorkRoot, Text: "archivedchild", NormalizedText: "archivedchild", SourceName: "mcp", SourceRank: 4, RawJSON: "{}", UpdatedAt: now}, nil))
				}
				_, err = st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", "workspace", "TLOCAL", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
				require.NoError(t, err)
				workspaceBefore := mcpWorkWorkspace(t, st)
				childBefore := apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkChild+"'")
				failedTS := "1710000501.000000"
				if text {
					failedTS = "1710000502.000000"
				}
				_, err = st.DB().ExecContext(ctx, "create trigger fail_later_history_batch before insert on messages when new.ts='"+failedTS+"' begin select raise(abort,'synthetic_later_batch_failure'); end")
				require.NoError(t, err)
				require.NoError(t, st.Close())
				args := []string{"--config", path, "--json", "sync", "--source", source, "--workspace", "TLOCAL", "--channels", "C123", "--with-media=false"}
				observations := make([]mcpWorkObservation, 0, 2)
				snapshots := make([]map[string][]map[string]any, 0, 2)
				for attempt := range 2 {
					fixture.mu.Lock()
					fixture.retry, fixture.calls = attempt == 1, []mcpCoverageCall{}
					fixture.mu.Unlock()
					var stdout, stderr bytes.Buffer
					runErr := (&App{Stdout: &stdout, Stderr: &stderr}).Run(ctx, args)
					st, err = store.OpenReadOnly(cfg.DBPath)
					require.NoError(t, err)
					observed := mcpWorkObservation{
						Error: userPrimaryError(runErr), Completion: stdout.Len() != 0,
						Pending: mcpWorkPending(t, st), Workspace: mcpWorkWorkspace(t, st),
						Rows:   apiKeyRows(t, st, "select ts,coalesce(thread_ts,'') as thread_ts,text,reply_count,source_name,source_rank from messages order by ts"),
						Counts: apiCompletenessCounts(t, st), FTS: apiKeyRows(t, st, "select message_key from message_fts order by message_key"),
					}
					fixture.mu.Lock()
					observed.Calls, observed.ServerErrors = append([]mcpCoverageCall{}, fixture.calls...), append([]string{}, fixture.errors...)
					fixture.mu.Unlock()
					require.NoError(t, st.Close())
					observations = append(observations, observed)
					snapshots = append(snapshots, shareArchiveSnapshot(t, cfg.DBPath))
				}
				first, retry := observations[0], observations[1]
				t.Logf("MCP batch observations: first_error=%q first_pending=%d first_rows=%d retry_error=%q retry_pending=%d retry_rows=%d", first.Error, len(first.Pending), len(first.Rows), retry.Error, len(retry.Pending), len(retry.Rows))
				require.Contains(t, first.Error, "synthetic_later_batch_failure")
				require.Len(t, first.Pending, 1, "committed child evidence must survive a later batch failure")
				require.Equal(t, retainedCLIKey("TLOCAL", "C123", mcpWorkRoot), first.Pending[0]["entity_id"])
				require.NotEmpty(t, first.Pending[0]["value"])
				require.Equal(t, mcpWorkToolError, retry.Error)
				require.Equal(t, first.Pending, retry.Pending, "a missing tool cannot renew or retire queued work")
				require.Equal(t, snapshots[0], snapshots[1], "the empty-history retry preserves all archive rows and derived state")
				wantRows := 500
				if text {
					wantRows++
				}
				for attempt, observed := range observations {
					require.Empty(t, observed.ServerErrors)
					require.False(t, observed.Completion)
					require.Equal(t, workspaceBefore, observed.Workspace)
					require.Len(t, observed.Rows, wantRows)
					require.Len(t, observed.FTS, wantRows)
					for _, table := range []string{"messages", "message_events", "message_event_heads", "message_fts"} {
						require.Equal(t, wantRows, observed.Counts[table], table)
					}
					wantCalls := []mcpCoverageCall{{"slack_list_channels", map[string]any{"limit": float64(20)}}, {"slack_get_channel_history", map[string]any{"channel_id": "C123", "limit": float64(501)}}}
					if text {
						oldest := "1709996402.000000"
						if attempt == 1 {
							oldest = "1709996901.000000"
						}
						wantCalls = []mcpCoverageCall{{"slack_read_channel", map[string]any{"channel_id": "C123", "oldest": oldest, "limit": float64(501), "response_format": "detailed"}}}
					}
					require.Equal(t, wantCalls, observed.Calls)
					for _, row := range observed.Rows {
						require.NotEqual(t, failedTS, row["ts"])
						require.Equal(t, "mcp", row["source_name"])
						require.Equal(t, int64(4), row["source_rank"])
						require.Equal(t, int64(0), row["reply_count"])
					}
				}
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				defer func() { require.NoError(t, st.Close()) }()
				require.Equal(t, []map[string]any{{"thread_ts": mcpWorkRoot}}, apiKeyRows(t, st, "select thread_ts from messages where ts='"+mcpWorkChild+"'"))
				require.Equal(t, retry.FTS, apiKeyRows(t, st, "select channel_id || '|' || ts as message_key from messages order by message_key"))
				if text {
					require.Equal(t, childBefore, apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkChild+"'"))
				}
				configAfter, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, configBefore, configAfter)
			})
		}
	}
}

type mcpBatchFixture struct {
	mu     sync.Mutex
	text   bool
	retry  bool
	calls  []mcpCoverageCall
	errors []string
}

func (f *mcpBatchFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "synthetic batch fixture rejected request", http.StatusBadRequest)
	}
	if r.Header.Get("Authorization") != "Bearer synthetic-batch-token" {
		fail("unexpected credential")
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail("invalid request JSON")
		return
	}
	f.mu.Lock()
	retry := f.retry
	f.mu.Unlock()
	var result any
	switch req.Method {
	case "initialize":
		result = map[string]any{"protocolVersion": "2025-03-26"}
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
		return
	case "tools/list":
		names := []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history"}
		thread := "slack_get_thread_replies"
		if f.text {
			names, thread = []string{"slack_search_channels", "slack_search_users", "slack_read_channel"}, "slack_read_thread"
		}
		if !retry {
			names = append(names, thread)
		}
		tools := []map[string]any{}
		for _, name := range names {
			tools = append(tools, map[string]any{"name": name})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		f.mu.Lock()
		f.calls = append(f.calls, mcpCoverageCall{req.Params.Name, req.Params.Arguments})
		f.mu.Unlock()
		payload := map[string]any{"ok": true}
		switch req.Params.Name {
		case "slack_list_channels":
			payload["channels"] = []map[string]any{{"id": "C123", "name": "fixture", "is_channel": true}}
		case "slack_get_channel_history", "slack_read_channel":
			if req.Params.Arguments["limit"] != float64(501) || req.Params.Arguments["channel_id"] != "C123" {
				fail("history request must explicitly ask for the 501-message page")
				return
			}
			messages := []map[string]any{}
			if !retry {
				messages = append(messages, mcpWorkMessage(mcpWorkRoot, "", false))
				if !f.text {
					messages = append(messages, mcpWorkMessage(mcpWorkChild, mcpWorkRoot, false))
				}
				for i := 3; len(messages) < 501; i++ {
					message := mcpWorkMessage(fmt.Sprintf("%d.000000", 1710000000+i), "", false)
					message["text"] = fmt.Sprintf("batchmessage%d", i)
					messages = append(messages, message)
				}
			}
			payload["messages"] = messages
			if f.text {
				payload = map[string]any{"messages": mcpWorkTextHistory(messages)}
			}
		default:
			fail("unexpected tool call")
			return
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			fail("response encoding failed")
			return
		}
		result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
	default:
		fail("unexpected method")
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
		f.mu.Lock()
		f.errors = append(f.errors, "response write failed")
		f.mu.Unlock()
	}
}
