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
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

// Keep this fixture on existing CLI/config/store APIs so it can also observe
// the parent's successful, but incomplete, coverage update.
func TestAPIHistoryCompletenessFromCLI(t *testing.T) {
	const priorLatest = "1709900000.000000"
	const attemptedOldest = "1709896400.000000"
	const parentTS = "1710000001.000000"
	const historyError = "conversations.history returned has_more without a continuation cursor; retry after resolving upstream pagination"
	const repliesError = "conversations.replies returned has_more without a continuation cursor; retry after resolving upstream pagination"
	const limitedError = "Slack reported a history/message limit; completeness of the requested interval is uncertified; review workspace history availability"
	for _, tc := range []struct {
		name, wantError string
	}{
		{"history-more", historyError},
		{"history-more-empty", historyError},
		{"replies-more", repliesError},
		{"replies-more-empty", repliesError},
		{"history-limited", limitedError},
		{"history-limited-earlier", limitedError},
		{"history-limited-bounded", limitedError},
		{"complete", ""},
		{"history-cursor-false", ""},
		{"history-cursor-absent", ""},
		{"replies-cursor-false", ""},
		{"replies-cursor-absent", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			cfg := config.Default()
			cfg.DBPath, cfg.CacheDir, cfg.LogDir = filepath.Join(dir, "archive.db"), filepath.Join(dir, "cache"), filepath.Join(dir, "logs")
			cfg.Share.RepoPath, cfg.Share.Remote, cfg.Share.AutoUpdate = filepath.Join(dir, "share"), "", false
			cfg.Slack.Desktop.Path, cfg.Slack.Desktop.Enabled = filepath.Join(dir, "desktop"), false
			cfg.Slack.App.Enabled, cfg.Slack.MCP.Enabled = false, false
			cfg.Slack.MCP.AuthPath = filepath.Join(dir, "unused-auth.json")
			cfg.Slack.Bot.TokenEnv, cfg.Slack.User.TokenEnv = "SLACRAWL_COMPLETENESS_BOT", "SLACRAWL_COMPLETENESS_USER"
			cfg.Sync.IncludeDMs, cfg.Sync.FileMedia, cfg.Sync.Concurrency = new(false), new(false), 1
			t.Setenv("SLACRAWL_COMPLETENESS_BOT", "fixture-bot")
			t.Setenv("SLACRAWL_COMPLETENESS_USER", "fixture-user")
			configPath := filepath.Join(dir, "config.toml")
			require.NoError(t, cfg.Save(configPath))
			loaded, err := config.Load(configPath)
			require.NoError(t, err)
			require.Equal(t, new(false), loaded.Sync.IncludeDMs)
			scope := ""
			if tc.name == "history-limited-bounded" {
				scope = attemptedOldest
			}
			key, err := json.Marshal([]string{"T123", "C123", scope})
			require.NoError(t, err)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			now := time.Unix(1710000000, 0).UTC()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.SetSyncState(ctx, "api-bot", "history_coverage_v1", string(key), fmt.Sprintf("{\"complete\":true,\"latest\":%q}", priorLatest)))
			require.NoError(t, st.SetSyncState(ctx, "api-bot", "workspace", "T123", "2020-01-01T00:00:00Z"))
			parent := slack.Message{Msg: slack.Msg{Channel: "C123", Type: "message", Timestamp: parentTS, Text: "history-parent", ReplyCount: 2}, SubMessage: &slack.Msg{Channel: "C123"}}
			// Matching the history projection makes the seeded parent a stable
			// event; its valid replies echo still changes source and rank.
			require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "C123", WorkspaceID: "T123", TS: parentTS, Text: parent.Text, NormalizedText: parent.Text, ReplyCount: 2, SourceName: "api-bot", SourceRank: 2, RawJSON: store.MarshalRaw(parent), UpdatedAt: now}, nil))
			beforeWorkspace := apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'")
			require.NoError(t, st.Close())

			var corrected atomic.Bool
			var mu sync.Mutex
			var calls []apiKeyCall
			var serverErrors []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				fail := func(reason string) {
					serverErrors = append(serverErrors, reason)
					http.Error(w, "synthetic fixture request rejected", http.StatusBadRequest)
				}
				if err := r.ParseForm(); err != nil {
					fail("invalid form")
					return
				}
				payload := map[string]any{"ok": true}
				switch r.URL.Path {
				case "/auth.test":
					payload["team_id"], payload["team"] = "T123", "Fixture"
				case "/conversations.list":
					if strings.Contains(r.Form.Get("types"), "im") {
						fail("unexpected DM discovery")
						return
					}
					payload["channels"] = []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true}}
				case "/users.list":
					payload["members"] = []any{}
				case "/conversations.history", "/conversations.replies":
					call := apiKeyCall{r.URL.Path, r.Form.Get("cursor"), r.Form.Get("oldest"), r.Form.Get("latest"), r.Form.Get("ts")}
					calls = append(calls, call)
					if r.Form.Get("channel") != "C123" {
						fail("unexpected conversation")
						return
					}
					if call.Path == "/conversations.history" {
						if r.Form.Get("token") != "fixture-bot" || (call.Cursor != "" && call.Cursor != "history-next") {
							fail("unexpected history source or page")
							return
						}
						first := []any{apiKeyMessage("1710000000.000000", "completeness-h1"), parent}
						last := []any{apiKeyMessage("1710000003.000000", "completeness-h2")}
						if tc.name == "history-more-empty" {
							last = []any{}
						}
						if tc.name == "history-cursor-absent" {
							last = append(first, last...)
							first = []any{}
						}
						payload["messages"] = last
						if call.Cursor == "" {
							payload["messages"] = first
							payload["response_metadata"] = map[string]any{"next_cursor": "history-next"}
							if tc.name != "history-cursor-absent" {
								payload["has_more"] = tc.name != "history-cursor-false"
							}
							payload["is_limited"] = tc.name == "history-limited-earlier" && !corrected.Load()
						} else {
							payload["has_more"] = strings.HasPrefix(tc.name, "history-more") && !corrected.Load()
							payload["is_limited"] = (tc.name == "history-limited" || tc.name == "history-limited-bounded") && !corrected.Load()
						}
					} else {
						if r.Form.Get("token") != "fixture-user" || call.TS != parentTS || (call.Cursor != "" && call.Cursor != "replies-next") {
							fail("unexpected replies source, parent or page")
							return
						}
						echo := parent
						echo.ThreadTimestamp = parentTS
						reply := apiKeyMessage("1710000002.000000", "completeness-r1")
						reply["thread_ts"] = parentTS
						first := []any{echo, reply}
						reply = apiKeyMessage("1710000004.000000", "completeness-r2")
						reply["thread_ts"] = parentTS
						last := []any{reply}
						if tc.name == "replies-more-empty" {
							last = []any{}
						}
						if tc.name == "replies-cursor-absent" {
							last = append(first, last...)
							first = []any{}
						}
						payload["messages"] = last
						if call.Cursor == "" {
							payload["messages"] = first
							payload["response_metadata"] = map[string]any{"next_cursor": "replies-next"}
							if tc.name != "replies-cursor-absent" {
								payload["has_more"] = tc.name != "replies-cursor-false"
							}
						} else {
							payload["has_more"] = strings.HasPrefix(tc.name, "replies-more") && !corrected.Load()
						}
					}
				default:
					fail("unexpected endpoint")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(payload); err != nil {
					serverErrors = append(serverErrors, "response write failed")
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: server.URL + "/", httpClient: server.Client()}
			args := []string{"--config", configPath, "--json", "sync", "--source", "api", "--with-media=false"}
			if scope != "" {
				args = append(args, "--since", scope)
			}
			runErr := app.Run(ctx, args)
			st, err = store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			coverage := apiCompletenessCoverage(t, st, string(key))
			workspacePreserved := reflect.DeepEqual(beforeWorkspace, apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			rows := apiKeyRows(t, st, "select ts,source_name,source_rank from messages order by ts")
			counts := apiCompletenessCounts(t, st)
			var result map[string]any
			outputErr := error(nil)
			if stdout.Len() > 0 {
				outputErr = json.Unmarshal(stdout.Bytes(), &result)
			}
			mu.Lock()
			observedCalls, observedErrors := append([]apiKeyCall(nil), calls...), append([]string(nil), serverErrors...)
			mu.Unlock()
			expectedError := runErr != nil && runErr.Error() == tc.wantError
			observation, err := json.Marshal(map[string]any{
				"error": runErr != nil, "expected_error": expectedError, "coverage": coverage,
				"workspace_preserved": workspacePreserved, "rows": rows, "counts": counts,
				"requests": observedCalls, "completion": result["status"] != nil, "output_error": outputErr != nil,
			})
			require.NoError(t, err)
			t.Logf("api completeness observation: %s", observation)
			require.Empty(t, observedErrors, "fixture infrastructure")
			require.NoError(t, outputErr, "CLI JSON")
			if tc.wantError != "" {
				if !expectedError || !workspacePreserved || coverage.Pending == nil || coverage.Latest != priorLatest || !coverage.Complete {
					t.Fatalf("api completeness boundary: error=%t expected_error=%t pending=%t workspace_preserved=%t prior_latest=%t complete=%t", runErr != nil, expectedError, coverage.Pending != nil, workspacePreserved, coverage.Latest == priorLatest, coverage.Complete)
				}
				require.Equal(t, attemptedOldest, *coverage.Pending)
				require.Empty(t, result, "incomplete CLI must not print success")
				require.Contains(t, stderr.String(), "state=failed")
				require.NotContains(t, stderr.String(), "state=finished")
			} else {
				require.NoError(t, runErr)
				require.False(t, workspacePreserved)
				require.Contains(t, result, "status")
				require.Nil(t, coverage.Pending)
				require.True(t, coverage.Complete)
			}
			wantTS := []string{"1710000000.000000", parentTS, "1710000002.000000", "1710000003.000000", "1710000004.000000"}
			if tc.name == "history-more-empty" || strings.HasPrefix(tc.name, "replies-more") {
				wantTS = append(wantTS[:3], wantTS[4:]...)
			}
			if tc.name == "replies-more-empty" {
				wantTS = wantTS[:3]
			}
			apiCompletenessAssertRows(t, st, wantTS)
			wantCalls := apiCompletenessCalls(observedCalls[0].Latest, attemptedOldest, tc.name == "history-cursor-absent")
			if strings.HasPrefix(tc.name, "replies-more") {
				wantCalls = wantCalls[:3]
			}
			require.Equal(t, wantCalls, observedCalls)
			if tc.wantError == "" {
				require.Equal(t, observedCalls[0].Latest, coverage.Latest)
				return
			}

			require.NoError(t, st.Close())
			corrected.Store(true)
			stdout.Reset()
			stderr.Reset()
			require.NoError(t, app.Run(ctx, args))
			st, err = store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			coverage = apiCompletenessCoverage(t, st, string(key))
			mu.Lock()
			retryCalls := append([]apiKeyCall(nil), calls[len(observedCalls):]...)
			observedErrors = append([]string(nil), serverErrors...)
			mu.Unlock()
			require.Empty(t, observedErrors)
			require.Equal(t, apiCompletenessCalls(retryCalls[0].Latest, attemptedOldest, false), retryCalls)
			require.True(t, coverage.Complete)
			require.Nil(t, coverage.Pending)
			require.Equal(t, retryCalls[0].Latest, coverage.Latest)
			require.NotEqual(t, priorLatest, coverage.Latest)
			require.NotEqual(t, beforeWorkspace, apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			wantTS = []string{"1710000000.000000", parentTS, "1710000002.000000", "1710000003.000000", "1710000004.000000"}
			if tc.name == "history-more-empty" {
				wantTS = append(wantTS[:3], wantTS[4:]...)
			}
			if tc.name == "replies-more-empty" {
				wantTS = wantTS[:4]
			}
			apiCompletenessAssertRows(t, st, wantTS)
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
			require.Contains(t, result, "status")
		})
	}
}

func apiCompletenessCoverage(t *testing.T, st *store.Store, key string) apiKeyCoverage {
	t.Helper()
	raw, err := st.GetSyncState(context.Background(), "api-bot", "history_coverage_v1", key)
	require.NoError(t, err)
	var coverage apiKeyCoverage
	require.NoError(t, json.Unmarshal([]byte(raw), &coverage))
	return coverage
}

func apiCompletenessCounts(t *testing.T, st *store.Store) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"messages", "message_events", "message_event_heads", "message_files", "message_mentions", "message_fts"} {
		counts[table] = len(apiKeyRows(t, st, "select * from "+table))
	}
	return counts
}

func apiCompletenessAssertRows(t *testing.T, st *store.Store, timestamps []string) {
	t.Helper()
	rows := apiKeyRows(t, st, "select ts,source_name,source_rank from messages order by ts")
	var actual []string
	for _, row := range rows {
		ts := row["ts"].(string)
		actual = append(actual, ts)
		if ts == "1710000000.000000" || ts == "1710000003.000000" {
			require.Equal(t, "api-bot", row["source_name"])
			require.EqualValues(t, 2, row["source_rank"])
		} else {
			require.Equal(t, "api-user", row["source_name"])
			require.EqualValues(t, 1, row["source_rank"])
		}
	}
	require.Equal(t, timestamps, actual)
	require.Equal(t, map[string]int{"messages": len(timestamps), "message_events": len(timestamps) + 1, "message_event_heads": len(timestamps) + 1, "message_files": len(timestamps) - 1, "message_mentions": len(timestamps) - 1, "message_fts": len(timestamps)}, apiCompletenessCounts(t, st))
	require.Len(t, apiKeyDerived(t, st), len(timestamps)-1)
	parent := apiKeyRows(t, st, "select raw_json from messages where ts='1710000001.000000'")
	require.Len(t, parent, 1)
	var message slack.Message
	require.NoError(t, json.Unmarshal([]byte(parent[0]["raw_json"].(string)), &message))
	require.Equal(t, "1710000001.000000", message.ThreadTimestamp)
	require.NotNil(t, message.SubMessage)
	require.Empty(t, message.SubMessage.Timestamp)
}

func apiCompletenessCalls(horizon, oldest string, historyEmptyFirst bool) []apiKeyCall {
	first := apiKeyCall{Path: "/conversations.history", Oldest: oldest, Latest: horizon}
	last := first
	last.Cursor = "history-next"
	replies := []apiKeyCall{
		{Path: "/conversations.replies", TS: "1710000001.000000"},
		{Path: "/conversations.replies", Cursor: "replies-next", TS: "1710000001.000000"},
	}
	if historyEmptyFirst {
		return append([]apiKeyCall{first, last}, replies...)
	}
	return append(append([]apiKeyCall{first}, replies...), last)
}
