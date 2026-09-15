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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const (
	mcpHistoryRoot  = "1710000000.000000"
	mcpHistoryGap   = "1710005000.000000"
	mcpHistoryReply = "1710020000.000000"
	mcpHistoryNew   = "1710020001.000000"
	mcpHistoryOld   = "1709900000.000000"
	mcpHistorySince = "1710010000.000000"
)

// Keep this regression on existing CLI/config/store APIs. Every restart and
// observation precedes the policy boundary, including on the unchanged parent.
func TestMCPHistoryCheckpointsFromCLI(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		text         bool
		attempts     int
	}{
		{"reply-newer-than-history", "mcp", true, 2},
		{"completed-empty-history", "connector", true, 3},
		{"later-channel-failure", "mcp", false, 1},
		{"native-incomplete-retry", "connector", false, 2},
		{"other-source-maximum", "mcp", true, 1},
		{"text-bootstrap", "mcp", true, 4},
		{"since", "connector", true, 1},
		{"full-since", "mcp", false, 1},
		{"full-then-retention", "connector", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := &mcpHistoryFixture{mode: tc.name, text: tc.text}
			server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
			defer server.Close()
			cfg, path := userPrimaryConfig(t)
			cfg.Slack.Bot.Enabled, cfg.Slack.User.Enabled, cfg.Slack.App.Enabled = false, false, false
			cfg.Sync.IncludeDMs = nil
			cfg.Slack.MCP.Enabled, cfg.Slack.MCP.Transport, cfg.Slack.MCP.BaseURL = true, "http", server.URL
			cfg.Slack.MCP.TokenEnv, cfg.Slack.MCP.AccountIDEnv = "SLACRAWL_HISTORY_TOKEN", "SLACRAWL_HISTORY_ACCOUNT"
			cfg.Slack.MCP.ConnectorID = ""
			cfg.Slack.MCP.PageSize, cfg.Slack.MCP.SearchLimit, cfg.Slack.MCP.MaxPages = 100, 20, 2
			t.Setenv(cfg.Slack.MCP.TokenEnv, "synthetic-history-token")
			t.Setenv(cfg.Slack.MCP.AccountIDEnv, "")
			require.NoError(t, cfg.Save(path))
			beforeConfig, err := os.ReadFile(path)
			require.NoError(t, err)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			seedTime := time.Unix(1700000000, 0).UTC()
			require.NoError(t, st.EnsureWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "history fixture", RawJSON: "{}", UpdatedAt: seedTime}))
			channels := []string{"CONE"}
			if tc.name == "later-channel-failure" {
				channels = append(channels, "CTWO")
			}
			for _, channel := range channels {
				require.NoError(t, st.EnsureChannel(ctx, store.Channel{ID: channel, WorkspaceID: "TLOCAL", Name: "history fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: seedTime}))
			}
			seedState := func(kind, key, value string) {
				_, err := st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "mcp", kind, key, value, "2020-01-01T00:00:00Z")
				require.NoError(t, err)
			}
			seedState("workspace", "TLOCAL", "2020-01-01T00:00:00Z")
			seedMessage := func(ts, source string, rank, replies int) {
				require.NoError(t, st.UpsertMessage(ctx, store.Message{WorkspaceID: "TLOCAL", ChannelID: "CONE", TS: ts, Text: "retained " + source, NormalizedText: "retained " + source, ReplyCount: replies, SourceName: source, SourceRank: rank, RawJSON: "{}", UpdatedAt: seedTime}, nil))
			}
			adapter := "reference"
			if tc.text {
				adapter = "codex"
			}
			sliced := tc.name == "since" || tc.name == "full-since"
			if sliced {
				seedMessage(mcpHistoryOld, "mcp", 4, 1)
				seedState("thread_pending_v1", retainedCLIKey("TLOCAL", "CONE", mcpHistoryOld), "seed-thread-generation")
				pending := "1709990000.000000"
				progress, err := json.Marshal(mcpHistoryProgress{Complete: true, Latest: mcpHistoryRoot, Pending: &pending, Revision: "seed-history-revision"})
				require.NoError(t, err)
				seedState("history_work_v1", mcpHistoryKey("CONE", adapter, ""), string(progress))
			}
			if tc.name == "other-source-maximum" || tc.name == "text-bootstrap" {
				seedMessage("1710030000.000000", "api-user", 1, 0)
				seedMessage("1710040000.000000", "desktop", 3, 0)
			}
			if tc.name == "native-incomplete-retry" {
				seedMessage(mcpHistoryRoot, "mcp", 4, 0)
				progress, err := json.Marshal(mcpHistoryProgress{Complete: true, Latest: mcpHistoryRoot, Revision: "seed-history-revision"})
				require.NoError(t, err)
				seedState("history_work_v1", mcpHistoryKey("CONE", adapter, ""), string(progress))
			}
			var purged []int64
			purge := func() {
				report, err := st.PurgeMessages(ctx, store.PurgeOptions{Before: time.Unix(1710000000, 0).UTC(), WorkspaceID: "TLOCAL", Delete: true, RequireNoMedia: true})
				require.NoError(t, err)
				purged = append(purged, report.Messages)
			}
			if tc.name == "full-then-retention" {
				purge()
			}
			beforeState := apiKeyRows(t, st, "select * from sync_state order by source_name,entity_type,entity_id")
			beforeMessages := apiKeyRows(t, st, "select * from messages order by channel_id,ts")
			require.NoError(t, st.Close())
			attempts := make([]mcpHistoryObservation, 0, tc.attempts)
			for attempt := 0; attempt < tc.attempts; attempt++ {
				if tc.name == "text-bootstrap" && attempt >= 2 {
					// Exercise the documented recovery with the same archive and
					// command; restore the original limit after bootstrap completes.
					cfg.Slack.MCP.MaxPages = 3
					if attempt == 3 {
						cfg.Slack.MCP.MaxPages = 2
					}
					require.NoError(t, cfg.Save(path))
				}
				fixture.mu.Lock()
				fixture.attempt, fixture.calls, fixture.errors = attempt, nil, nil
				fixture.mu.Unlock()
				args := []string{"--config", path, "--no-color", "sync", "--source", tc.source, "--workspace", "TLOCAL", "--channels", strings.Join(channels, ","), "--with-media=false"}
				if sliced {
					args = append(args, "--since", time.Unix(1710010000, 0).UTC().Format(time.RFC3339))
				}
				if tc.name == "full-since" || tc.name == "full-then-retention" && attempt == 0 {
					args = append(args, "--full")
				}
				var stdout, stderr bytes.Buffer
				runErr := (&App{Stdout: &stdout, Stderr: &stderr}).Run(ctx, args)
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				observation := mcpHistoryObservation{
					Error: userPrimaryError(runErr), Completed: strings.Contains(stdout.String(), "Completed"),
					Messages: apiKeyRows(t, st, "select channel_id,ts,coalesce(thread_ts,'') as thread_ts,text,reply_count,coalesce(latest_reply,'') as latest_reply,source_name,source_rank from messages order by channel_id,ts"),
					State:    apiKeyRows(t, st, "select * from sync_state order by source_name,entity_type,entity_id"),
					FTS:      apiKeyRows(t, st, "select message_key from message_fts order by message_key"),
					Counts:   map[string]int{},
				}
				var retained []map[string]any
				for _, row := range apiKeyRows(t, st, "select * from messages order by channel_id,ts") {
					for _, before := range beforeMessages {
						if row["channel_id"] == before["channel_id"] && row["ts"] == before["ts"] {
							retained = append(retained, row)
						}
					}
				}
				observation.RetainedPreserved = len(beforeMessages) == 0 || reflect.DeepEqual(beforeMessages, retained)
				for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state"} {
					observation.Counts[table] = len(apiKeyRows(t, st, "select * from "+table))
				}
				status, err := st.Status(ctx)
				require.NoError(t, err)
				observation.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339Nano)
				fixture.mu.Lock()
				observation.Calls = append([]mcpCoverageCall{}, fixture.calls...)
				observation.ServerErrors = append([]string{}, fixture.errors...)
				fixture.mu.Unlock()
				attempts = append(attempts, observation)
				require.NoError(t, st.Close())
				if tc.name == "full-then-retention" && attempt == 0 {
					st, err = store.Open(cfg.DBPath)
					require.NoError(t, err)
					purge()
					require.NoError(t, st.Close())
				}
			}
			afterConfig, err := os.ReadFile(path)
			require.NoError(t, err)
			observed, err := json.Marshal(map[string]any{"before_state": beforeState, "before_messages": beforeMessages, "attempts": attempts, "purged": purged, "config_preserved": bytes.Equal(beforeConfig, afterConfig)})
			require.NoError(t, err)
			t.Logf("MCP history observation: %s", observed)

			var failures []string
			check := func(ok bool, label string) {
				if !ok {
					failures = append(failures, label)
				}
			}
			check(bytes.Equal(beforeConfig, afterConfig), "config changed")
			for i, attempt := range attempts {
				prefix := fmt.Sprintf("attempt %d: ", i)
				wantError := ""
				if tc.name == "later-channel-failure" {
					wantError = "read MCP channel: MCP tools/call returned HTTP 503"
				}
				if tc.name == "native-incomplete-retry" && i == 0 {
					wantError = "native MCP history or replies are incomplete; received messages were processed without advancing successful sync state; use --source api for paginated backfill"
				}
				if tc.name == "text-bootstrap" && i < 2 {
					wantError = "read MCP channel: MCP pagination exceeded max_pages=2; history checkpoint remains pending; increase slack.mcp.max_pages temporarily, rerun the same sync, then restore the limit after completion"
				}
				check(attempt.Error == wantError, prefix+"unexpected error")
				check(attempt.Completed == (wantError == ""), prefix+"completion")
				check(len(attempt.ServerErrors) == 0, prefix+"fixture request rejected")
				check(attempt.RetainedPreserved, prefix+"retained seed rows changed")
				check(reflect.DeepEqual(attempt.Calls, mcpHistoryExpectedCalls(tc.name, tc.text, i)), prefix+"request bounds or thread scope")
				keys := make([]map[string]any, 0, len(attempt.Messages))
				for _, message := range attempt.Messages {
					keys = append(keys, map[string]any{"message_key": message["channel_id"].(string) + "|" + message["ts"].(string)})
				}
				check(len(keys) == 0 && len(attempt.FTS) == 0 || reflect.DeepEqual(keys, attempt.FTS), prefix+"FTS keys")
				workspace := mcpHistoryStateRows(attempt.State, "workspace", "TLOCAL")
				previous := mcpHistoryStateRows(beforeState, "workspace", "TLOCAL")
				check(reflect.DeepEqual(workspace, previous) == (wantError != ""), prefix+"workspace freshness")
				if wantError != "" {
					check(attempt.LastSyncAt == "2020-01-01T00:00:00Z", prefix+"progress advanced status freshness")
				}
			}
			progress := func(attempt int, channel string, complete bool, latest string, pending *string) mcpHistoryProgress {
				rows := mcpHistoryStateRows(attempts[attempt].State, "history_work_v1", mcpHistoryKey(channel, adapter, ""))
				var state mcpHistoryProgress
				valid := len(rows) == 1
				if valid {
					value, ok := rows[0]["value"].(string)
					valid = ok && json.Unmarshal([]byte(value), &state) == nil
					updatedAt, ok := rows[0]["updated_at"].(string)
					_, timeErr := time.Parse(time.RFC3339Nano, updatedAt)
					valid = valid && ok && timeErr == nil
				}
				check(valid && state.Revision != "" && state.Complete == complete && state.Latest == latest && reflect.DeepEqual(state.Pending, pending), fmt.Sprintf("attempt %d: %s history checkpoint", attempt, channel))
				return state
			}
			hasMessage := func(attempt int, ts, thread, source string, rank int64) bool {
				for _, row := range attempts[attempt].Messages {
					if row["channel_id"] == "CONE" && row["ts"] == ts && row["thread_ts"] == thread && row["source_name"] == source && row["source_rank"] == rank {
						return true
					}
				}
				return false
			}
			switch tc.name {
			case "reply-newer-than-history":
				progress(0, "CONE", true, mcpHistoryRoot, nil)
				progress(1, "CONE", true, mcpHistoryGap, nil)
				check(hasMessage(0, mcpHistoryReply, mcpHistoryRoot, "mcp", 4), "newer reply missing")
				check(hasMessage(1, mcpHistoryGap, "", "mcp", 4), "history gap skipped after newer reply")
			case "completed-empty-history":
				progress(0, "CONE", true, "", nil)
				progress(1, "CONE", true, mcpHistoryRoot, nil)
				progress(2, "CONE", true, mcpHistoryRoot, nil)
				check(len(attempts[0].Messages) == 0 && len(attempts[2].Messages) == 1, "empty history changed retained messages")
			case "later-channel-failure":
				progress(0, "CONE", true, mcpHistoryRoot, nil)
				progress(0, "CTWO", false, "", new(""))
				check(hasMessage(0, mcpHistoryRoot, "", "mcp", 4), "first channel commit lost")
			case "native-incomplete-retry":
				first := progress(0, "CONE", true, mcpHistoryRoot, new("1709996400.000000"))
				second := progress(1, "CONE", true, mcpHistoryReply, nil)
				check(first.Revision != "seed-history-revision" && first.Revision != second.Revision, "retry did not acquire a fresh history revision")
				check(hasMessage(0, mcpHistoryReply, "", "mcp", 4), "valid incomplete page lost")
				check(hasMessage(1, mcpHistoryGap, "", "mcp", 4), "pending interval gap skipped")
			case "other-source-maximum":
				progress(0, "CONE", true, mcpHistoryRoot, nil)
				check(hasMessage(0, mcpHistoryRoot, "", "mcp", 4), "foreign-source maximum hid MCP history")
				check(hasMessage(0, "1710030000.000000", "", "api-user", 1) && hasMessage(0, "1710040000.000000", "", "desktop", 3), "other-source rows changed")
			case "text-bootstrap":
				first := progress(0, "CONE", false, "", new(""))
				second := progress(1, "CONE", false, "", new(""))
				check(first.Revision != second.Revision, "failed retry did not renew history ownership")
				for i := 0; i < 2; i++ {
					check(len(attempts[i].Messages) == 2, "capped history was partially committed")
				}
				for i := 2; i < 4; i++ {
					progress(i, "CONE", true, mcpHistoryNew, nil)
					for _, ts := range []string{mcpHistoryRoot, mcpHistoryGap, mcpHistoryNew} {
						check(hasMessage(i, ts, "", "mcp", 4), "bootstrap lost a history page")
					}
					check(len(attempts[i].Messages) == 5, "bootstrap or incremental retry duplicated history")
				}
			case "since", "full-since":
				for _, kind := range []string{"history_work_v1", "thread_pending_v1"} {
					key := mcpHistoryKey("CONE", adapter, "")
					if kind == "thread_pending_v1" {
						key = retainedCLIKey("TLOCAL", "CONE", mcpHistoryOld)
					}
					check(reflect.DeepEqual(mcpHistoryStateRows(beforeState, kind, key), mcpHistoryStateRows(attempts[0].State, kind, key)), "sliced sync changed ordinary "+kind)
				}
				check(hasMessage(0, "1710020002.000000", mcpHistoryNew, "mcp", 4), "sliced current replies missing")
				check(!hasMessage(0, "1709900001.000000", mcpHistoryOld, "mcp", 4), "sliced sync visited old thread")
			case "full-then-retention":
				check(reflect.DeepEqual(purged, []int64{0, 1}), "retention setup or purge")
				check(hasMessage(0, mcpHistoryOld, "", "mcp", 4), "Full did not restore older history")
				check(!hasMessage(1, mcpHistoryOld, "", "mcp", 4), "ordinary retry inherited Full authority")
				check(hasMessage(1, "1710000100.000000", "", "mcp", 4), "retained history missing")
			}
			if len(failures) > 0 {
				t.Fatalf("MCP history checkpoint boundary: %s", strings.Join(failures, "; "))
			}
		})
	}
}

// Pending null means idle; a non-nil empty string is unfinished unbounded work.
// Complete plus empty Latest records a completed empty history, not absence.
type mcpHistoryProgress struct {
	Complete bool    `json:"complete"`
	Latest   string  `json:"latest"`
	Pending  *string `json:"pending"`
	Revision string  `json:"revision"`
}

type mcpHistoryObservation struct {
	Error             string            `json:"error"`
	Completed         bool              `json:"completed"`
	Calls             []mcpCoverageCall `json:"calls"`
	Messages          []map[string]any  `json:"messages"`
	State             []map[string]any  `json:"state"`
	FTS               []map[string]any  `json:"fts"`
	Counts            map[string]int    `json:"counts"`
	LastSyncAt        string            `json:"last_sync_at"`
	ServerErrors      []string          `json:"server_errors"`
	RetainedPreserved bool              `json:"retained_preserved"`
}

func mcpHistoryKey(channel, adapter, since string) string {
	key, _ := json.Marshal([]string{"TLOCAL", channel, adapter, since})
	return string(key)
}

func mcpHistoryStateRows(rows []map[string]any, kind, key string) []map[string]any {
	var found []map[string]any
	for _, row := range rows {
		if row["source_name"] == "mcp" && row["entity_type"] == kind && row["entity_id"] == key {
			found = append(found, row)
		}
	}
	return found
}

func mcpHistoryExpectedCalls(mode string, text bool, attempt int) []mcpCoverageCall {
	oldest := ""
	if mode == "since" || mode == "full-since" {
		oldest = mcpHistorySince
	} else if mode == "reply-newer-than-history" && attempt == 1 || mode == "completed-empty-history" && attempt == 2 {
		oldest = "1709996400.000000"
	} else if mode == "full-then-retention" && attempt == 1 {
		oldest = "1709999999.999999"
	} else if mode == "text-bootstrap" && attempt == 3 {
		oldest = "1710016401.000000"
	}
	history := func(channel string) mcpCoverageCall {
		args := map[string]any{"channel_id": channel, "limit": float64(100)}
		name := "slack_get_channel_history"
		if text {
			name, args["response_format"] = "slack_read_channel", "detailed"
			if oldest != "" {
				args["oldest"] = oldest
			}
		}
		return mcpCoverageCall{name, args}
	}
	calls := []mcpCoverageCall{history("CONE")}
	if mode == "text-bootstrap" && attempt < 3 {
		pages := 2
		if attempt == 2 {
			pages = 3
		}
		for page := 1; page < pages; page++ {
			call := history("CONE")
			call.Args["cursor"] = fmt.Sprintf("bootstrap-page-%d", page)
			calls = append(calls, call)
		}
	}
	if mode == "reply-newer-than-history" || mode == "since" || mode == "full-since" {
		root := mcpHistoryRoot
		if mode != "reply-newer-than-history" {
			root = mcpHistoryNew
		}
		call := mcpCoverageCall{"slack_get_thread_replies", map[string]any{"channel_id": "CONE", "thread_ts": root}}
		if text {
			call = mcpCoverageCall{"slack_read_thread", map[string]any{"channel_id": "CONE", "message_ts": root, "limit": float64(100), "response_format": "detailed"}}
		}
		calls = append(calls, call)
	}
	if mode == "later-channel-failure" {
		calls = append(calls, history("CTWO"))
	}
	return calls
}

type mcpHistoryFixture struct {
	mu      sync.Mutex
	mode    string
	text    bool
	attempt int
	calls   []mcpCoverageCall
	errors  []string
}

func (f *mcpHistoryFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage
		Method string
		Params struct {
			Name      string
			Arguments map[string]any
		}
	}
	fail := func(reason string) {
		f.mu.Lock()
		f.errors = append(f.errors, reason)
		f.mu.Unlock()
		http.Error(w, "synthetic history fixture rejected request", http.StatusBadRequest)
	}
	if r.Header.Get("Authorization") != "Bearer synthetic-history-token" {
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
		names := []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history", "slack_get_thread_replies"}
		if f.text {
			names = []string{"slack_search_channels", "slack_search_users", "slack_read_channel", "slack_read_thread"}
		}
		var tools []map[string]any
		for _, name := range names {
			tools = append(tools, map[string]any{"name": name})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		f.mu.Lock()
		f.calls = append(f.calls, mcpCoverageCall{req.Params.Name, req.Params.Arguments})
		attempt := f.attempt
		f.mu.Unlock()
		channel, _ := req.Params.Arguments["channel_id"].(string)
		if channel != "CONE" && !(channel == "CTWO" && f.mode == "later-channel-failure") {
			fail("unexpected channel")
			return
		}
		message := func(ts, thread string, hint bool) map[string]any {
			row := map[string]any{"channel": channel, "context_team_id": "TLOCAL", "ts": ts, "thread_ts": thread, "user": "UFIXTURE", "text": "historymessage " + ts}
			if hint {
				row["reply_count"] = 1
				row["latest_reply"] = map[string]string{mcpHistoryRoot: mcpHistoryReply, mcpHistoryNew: "1710020002.000000", mcpHistoryOld: "1709900001.000000"}[ts]
			}
			return row
		}
		payload := map[string]any{"ok": true}
		switch req.Params.Name {
		case "slack_get_channel_history", "slack_read_channel":
			if f.mode == "later-channel-failure" && channel == "CTWO" {
				http.Error(w, "synthetic later channel failure", http.StatusServiceUnavailable)
				return
			}
			messages := []map[string]any{}
			switch f.mode {
			case "reply-newer-than-history":
				if attempt == 0 {
					messages = append(messages, message(mcpHistoryRoot, "", true))
				} else {
					messages = append(messages, message(mcpHistoryGap, "", false))
				}
			case "completed-empty-history":
				if attempt == 1 {
					messages = append(messages, message(mcpHistoryRoot, "", false))
				}
			case "later-channel-failure", "other-source-maximum":
				messages = append(messages, message(mcpHistoryRoot, "", false))
			case "text-bootstrap":
				// The newest raw-history timestamp is on the first page, while
				// the archive contains still newer API/Desktop rows.
				for _, ts := range []string{mcpHistoryNew, mcpHistoryGap, mcpHistoryRoot} {
					messages = append(messages, message(ts, "", false))
				}
			case "native-incomplete-retry":
				messages = append(messages, message(mcpHistoryReply, "", false))
				if attempt == 0 {
					payload["has_more"] = true
				} else {
					messages = append(messages, message(mcpHistoryGap, "", false))
				}
			case "since", "full-since":
				messages = append(messages, message(mcpHistoryOld, "", true), message(mcpHistoryNew, "", true))
			case "full-then-retention":
				messages = append(messages, message(mcpHistoryOld, "", false), message("1710000100.000000", "", false))
			}
			payload["messages"] = messages
			if f.text {
				// Model the server's text-history bound. Native history instead
				// returns the same page and lets the adapter apply its local bound.
				oldest, _ := req.Params.Arguments["oldest"].(string)
				lower, err := strconv.ParseFloat(oldest, 64)
				if oldest != "" && err != nil {
					fail("invalid oldest")
					return
				}
				pagination := ""
				if f.mode == "text-bootstrap" {
					var eligible []map[string]any
					for _, row := range messages {
						ts, _ := strconv.ParseFloat(row["ts"].(string), 64)
						if oldest == "" || ts >= lower {
							eligible = append(eligible, row)
						}
					}
					page := 0
					if cursor, _ := req.Params.Arguments["cursor"].(string); cursor != "" {
						page, err = strconv.Atoi(strings.TrimPrefix(cursor, "bootstrap-page-"))
						if err != nil || page < 1 || page >= len(eligible) {
							fail("unexpected history cursor")
							return
						}
					}
					messages = eligible
					if len(eligible) > 0 {
						messages = eligible[page : page+1]
					}
					if page+1 < len(eligible) {
						pagination = fmt.Sprintf("cursor `bootstrap-page-%d`", page+1)
					}
				}
				var body strings.Builder
				fmt.Fprintf(&body, "Channel: #fixture (%s)", channel)
				for _, row := range messages {
					ts, _ := strconv.ParseFloat(row["ts"].(string), 64)
					if oldest != "" && ts < lower {
						continue
					}
					fmt.Fprintf(&body, "\n\n=== Message from Fixture (UFIXTURE) at 2024-03-09T16:00:00Z === \nMessage TS: %s\n%s", row["ts"], row["text"])
					if row["reply_count"] == 1 {
						fmt.Fprintf(&body, "\nThread: 1 replies (latest: %s)", row["latest_reply"])
					}
				}
				payload = map[string]any{"messages": body.String(), "pagination_info": pagination}
			}
		case "slack_get_thread_replies", "slack_read_thread":
			root, _ := req.Params.Arguments["thread_ts"].(string)
			if f.text {
				root, _ = req.Params.Arguments["message_ts"].(string)
			}
			child := ""
			switch root {
			case mcpHistoryRoot:
				child = mcpHistoryReply
			case mcpHistoryNew:
				child = "1710020002.000000"
			case mcpHistoryOld:
				child = "1709900001.000000"
			default:
				fail("unexpected thread root")
				return
			}
			payload["messages"] = []map[string]any{message(root, "", false), message(child, root, false)}
			if f.text {
				payload = map[string]any{"messages": fmt.Sprintf("From: Fixture (UFIXTURE)\nTime: 2024-03-09T16:00:00Z\nMessage TS: %s\nhistory parent\n\n=== THREAD REPLIES ===\n\n--- Reply 1 ---\nFrom: Fixture (UFIXTURE)\nTime: 2024-03-09T16:00:01Z\nMessage TS: %s\nhistory reply", root, child)}
			}
		default:
			fail("unexpected tool")
			return
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			fail("marshal response")
			return
		}
		result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
	default:
		fail("unexpected method")
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
		f.mu.Lock()
		f.errors = append(f.errors, "response write")
		f.mu.Unlock()
	}
}
