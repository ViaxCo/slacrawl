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

// This fixture uses existing CLI/config/store APIs so the unchanged parent
// reaches the missing-key boundary, including its ordinary success path.
func TestAPIMessageKeyAdmissionFromCLI(t *testing.T) {
	const priorLatest = "1709900000.000000"
	const attemptedOldest = "1709896400.000000"
	const parentTS = "1710000001.000000"
	for _, tc := range []struct {
		name, endpoint, timestamp string
		valid                     bool
	}{
		{"history-empty", "history", "", false},
		{"history-whitespace", "history", " \t ", false},
		{"replies-empty", "replies", "", false},
		{"replies-whitespace", "replies", " \t ", false},
		{"valid-parent", "replies", "1710000004.000000", true},
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
			cfg.Slack.Bot.TokenEnv, cfg.Slack.User.TokenEnv = "SLACRAWL_KEY_BOT", "SLACRAWL_KEY_USER"
			cfg.Sync.IncludeDMs, cfg.Sync.FileMedia, cfg.Sync.Concurrency = new(false), new(false), 1
			t.Setenv("SLACRAWL_KEY_BOT", "fixture-bot")
			t.Setenv("SLACRAWL_KEY_USER", "fixture-user")
			configPath := filepath.Join(dir, "config.toml")
			require.NoError(t, cfg.Save(configPath))
			loaded, err := config.Load(configPath)
			require.NoError(t, err)
			require.Equal(t, new(false), loaded.Sync.IncludeDMs)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			now := time.Unix(1710000000, 0).UTC()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.SetSyncState(ctx, "api-bot", "history_coverage_v1", `["T123","C123",""]`, `{"complete":true,"latest":"`+priorLatest+`"}`))
			_, err = st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", "api-bot", "workspace", "T123", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			parent := slack.Message{Msg: slack.Msg{Channel: "C123", Type: "message", Timestamp: parentTS, Text: "history-parent", ReplyCount: 2}, SubMessage: &slack.Msg{Channel: "C123"}}
			if tc.endpoint == "replies" {
				// Match the API projection so replaying history does not create an
				// unrelated parent event before the malformed replies page arrives.
				require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "C123", WorkspaceID: "T123", TS: parentTS, Text: parent.Text, NormalizedText: parent.Text, ReplyCount: 2, SourceName: "api-bot", SourceRank: 2, RawJSON: store.MarshalRaw(parent), UpdatedAt: now}, nil))
			}
			beforeWorkspace := apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'")
			beforeParent := apiKeyParent(t, st)
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
					payload["channels"] = []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true, "latest": map[string]any{"channel": "C123"}}}
				case "/users.list":
					payload["members"] = []any{}
				case "/conversations.history", "/conversations.replies":
					call := apiKeyCall{r.URL.Path, r.Form.Get("cursor"), r.Form.Get("oldest"), r.Form.Get("latest"), r.Form.Get("ts")}
					calls = append(calls, call)
					if r.Form.Get("channel") != "C123" || (call.Cursor != "" && call.Cursor != "second") {
						fail("unexpected channel or page")
						return
					}
					badTS := tc.timestamp
					if corrected.Load() {
						badTS = "1710000004.000000"
					}
					bad := apiKeyMessage(badTS, "message-key-bad")
					if call.Path == "/conversations.history" {
						if tc.endpoint == "replies" {
							payload["messages"] = []any{parent}
						} else if call.Cursor == "" {
							payload["messages"] = []any{apiKeyMessage("1710000000.000000", "earlier-page")}
							payload["response_metadata"] = map[string]any{"next_cursor": "second"}
						} else {
							sibling := apiKeyMessage("1710000003.000000", "message-key-sibling")
							sibling["reply_count"] = 1
							payload["messages"] = []any{sibling, bad}
						}
					} else if tc.endpoint == "history" && call.TS == "1710000003.000000" {
						// The old implementation must finish normally, not fail because
						// the fixture omitted the premature thread request it permits.
						payload["messages"] = []any{}
					} else if call.TS != parentTS {
						fail("unexpected thread")
						return
					} else if call.Cursor == "" {
						reply := apiKeyMessage("1710000002.000000", "earlier-reply")
						reply["thread_ts"] = parentTS
						payload["messages"] = []any{reply}
						payload["response_metadata"] = map[string]any{"next_cursor": "second"}
					} else {
						echo := parent
						echo.Text, echo.ThreadTimestamp = "reply-parent-replacement", parentTS
						sibling := apiKeyMessage("1710000003.000000", "message-key-sibling")
						sibling["thread_ts"] = parentTS
						bad["thread_ts"] = parentTS
						payload["messages"] = []any{echo, sibling, bad}
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
			runErr := app.Run(ctx, args)
			st, err = store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			coverage := apiKeyReadCoverage(t, st)
			workspacePreserved := reflect.DeepEqual(beforeWorkspace, apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			parentPreserved := reflect.DeepEqual(beforeParent, apiKeyParent(t, st))
			rows := apiKeyRows(t, st, "select ts,source_name,source_rank,text,raw_json from messages order by ts")
			badRows := apiKeyRows(t, st, "select ts from messages where text like 'message-key-bad%'")
			counts, leaked := apiKeyCanaries(t, st, stdout.String()+stderr.String()+fmt.Sprint(runErr))
			var result map[string]any
			if stdout.Len() > 0 {
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
			}
			mu.Lock()
			observedCalls, observedErrors := append([]apiKeyCall(nil), calls...), append([]string(nil), serverErrors...)
			mu.Unlock()
			coverageJSON, err := json.Marshal(coverage)
			require.NoError(t, err)
			t.Logf("message-key observation: error=%t coverage=%s rows=%d events=%d files=%d mentions=%d parent_preserved=%t workspace_preserved=%t requests=%+v", runErr != nil, coverageJSON, len(rows), counts["message_events"], counts["message_files"], counts["message_mentions"], parentPreserved, workspacePreserved, observedCalls)
			require.Empty(t, observedErrors, "fixture infrastructure")
			if !tc.valid {
				expectedError := "channel C123 " + tc.endpoint + ": message is missing a timestamp"
				if runErr == nil || runErr.Error() != expectedError || !workspacePreserved || coverage.Pending == nil || len(badRows) != 0 || !parentPreserved {
					t.Fatalf("message-key boundary: error=%t expected_error=%t pending=%t workspace_preserved=%t bad_rows=%d parent_preserved=%t", runErr != nil, runErr != nil && runErr.Error() == expectedError, coverage.Pending != nil, workspacePreserved, len(badRows), parentPreserved)
				}
				require.Equal(t, priorLatest, coverage.Latest)
				require.True(t, coverage.Complete)
				require.Equal(t, attemptedOldest, *coverage.Pending)
				require.Empty(t, result, "failed CLI must not print a success result")
				require.Contains(t, stderr.String(), "state=failed")
				require.NotContains(t, stderr.String(), "state=finished")
				require.False(t, leaked, "bad-page payloads must not reach any archive table or diagnostics")
				if tc.endpoint == "history" {
					require.Len(t, rows, 1)
					require.Equal(t, "1710000000.000000", rows[0]["ts"])
					require.Len(t, observedCalls, 2, "no thread work from the rejected history page")
					require.Equal(t, []string{"1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE"}, apiKeyDerived(t, st))
				} else {
					require.Len(t, rows, 2)
					require.Equal(t, "1710000002.000000", rows[1]["ts"])
					require.Equal(t, "api-user", rows[1]["source_name"])
					require.EqualValues(t, 1, rows[1]["source_rank"])
					require.Len(t, observedCalls, 3)
					require.Equal(t, []string{"1710000002.000000|FEARLIERREPLY|earlier-reply.txt|UEARLIERREPLY"}, apiKeyDerived(t, st))
				}
				require.Equal(t, len(rows), counts["message_events"])
				require.Equal(t, len(rows), counts["message_event_heads"])
				require.Equal(t, len(rows), counts["message_fts"])
				require.NoError(t, st.Close())
				corrected.Store(true)
				stdout.Reset()
				stderr.Reset()
				require.NoError(t, app.Run(ctx, args))
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				coverage = apiKeyReadCoverage(t, st)
				mu.Lock()
				observedCalls, observedErrors = append([]apiKeyCall(nil), calls...), append([]string(nil), serverErrors...)
				mu.Unlock()
				require.Empty(t, observedErrors)
			} else {
				require.NoError(t, runErr)
				require.False(t, parentPreserved, "valid native parent echoes update the stored parent")
				require.False(t, workspacePreserved)
			}
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
			require.Contains(t, result, "status")
			require.True(t, coverage.Complete)
			require.Nil(t, coverage.Pending)
			var starts []apiKeyCall
			for _, call := range observedCalls {
				if call.Path == "/conversations.history" {
					require.Equal(t, attemptedOldest, call.Oldest)
					if call.Cursor == "" {
						starts = append(starts, call)
					}
				}
			}
			wantStarts := 2
			if tc.valid {
				wantStarts = 1
			}
			require.Len(t, starts, wantStarts)
			require.Equal(t, starts[len(starts)-1].Latest, coverage.Latest)
			require.NotEqual(t, priorLatest, coverage.Latest)
			require.NotEqual(t, beforeWorkspace, apiKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			wantRows, wantEvents := 3, 3
			firstDerived := "1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE"
			if tc.endpoint == "replies" {
				wantRows, wantEvents = 4, 5
				firstDerived = "1710000002.000000|FEARLIERREPLY|earlier-reply.txt|UEARLIERREPLY"
			}
			require.Len(t, apiKeyRows(t, st, "select ts from messages"), wantRows)
			require.Len(t, apiKeyRows(t, st, "select * from message_events"), wantEvents)
			require.Len(t, apiKeyRows(t, st, "select * from message_event_heads"), wantEvents)
			require.Len(t, apiKeyRows(t, st, "select * from message_fts"), wantRows)
			require.Equal(t, []string{firstDerived, "1710000003.000000|FMESSAGEKEYSIBLING|message-key-sibling.txt|UMESSAGEKEYSIBLING", "1710000004.000000|FMESSAGEKEYBAD|message-key-bad.txt|UMESSAGEKEYBAD"}, apiKeyDerived(t, st))
			if tc.endpoint == "replies" {
				parentRows := apiKeyRows(t, st, "select ts,source_name,source_rank,text,raw_json from messages where ts='1710000001.000000'")
				require.Equal(t, "api-user", parentRows[0]["source_name"])
				require.EqualValues(t, 1, parentRows[0]["source_rank"])
				require.Equal(t, "reply-parent-replacement", parentRows[0]["text"])
				var stored slack.Message
				require.NoError(t, json.Unmarshal([]byte(parentRows[0]["raw_json"].(string)), &stored))
				require.Equal(t, parentTS, stored.Timestamp)
				require.Equal(t, parentTS, stored.ThreadTimestamp)
				require.NotNil(t, stored.SubMessage)
				require.Empty(t, stored.SubMessage.Timestamp)
			}
		})
	}
}

type apiKeyCall struct{ Path, Cursor, Oldest, Latest, TS string }

type apiKeyCoverage struct {
	Complete bool    `json:"complete"`
	Latest   string  `json:"latest"`
	Pending  *string `json:"pending"`
}

func apiKeyMessage(ts, text string) map[string]any {
	suffix := strings.ToUpper(strings.ReplaceAll(text, "-", ""))
	return map[string]any{
		"channel": "C123", "type": "message", "ts": ts, "text": text + " <@U" + suffix + ">",
		"files": []any{map[string]any{"id": "F" + suffix, "name": text + ".txt", "title": text}},
	}
}

func apiKeyRows(t *testing.T, st *store.Store, query string) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), query)
	require.NoError(t, err, query)
	return rows
}

func apiKeyReadCoverage(t *testing.T, st *store.Store) apiKeyCoverage {
	t.Helper()
	raw, err := st.GetSyncState(context.Background(), "api-bot", "history_coverage_v1", `["T123","C123",""]`)
	require.NoError(t, err)
	var coverage apiKeyCoverage
	require.NoError(t, json.Unmarshal([]byte(raw), &coverage))
	return coverage
}

func apiKeyParent(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	return map[string][]map[string]any{
		"message": apiKeyRows(t, st, "select ts,source_name,source_rank,text,normalized_text,reply_count,thread_ts,raw_json from messages where ts='1710000001.000000'"),
		"events":  apiKeyRows(t, st, "select * from message_events where ts='1710000001.000000' order by id"),
		"heads":   apiKeyRows(t, st, "select * from message_event_heads where ts='1710000001.000000' order by channel_id,ts,event_type,source_name"),
	}
}

func apiKeyCanaries(t *testing.T, st *store.Store, output string) (map[string]int, bool) {
	t.Helper()
	counts := map[string]int{}
	leaked := false
	for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "sync_state", "message_mentions", "embedding_jobs", "message_fts"} {
		rows := apiKeyRows(t, st, "select * from "+table)
		counts[table] = len(rows)
		for _, canary := range []string{"message-key-bad", "message-key-sibling", "reply-parent-replacement", "UMESSAGEKEYBAD", "UMESSAGEKEYSIBLING", "FMESSAGEKEYBAD", "FMESSAGEKEYSIBLING"} {
			leaked = leaked || strings.Contains(fmt.Sprint(rows), canary) || strings.Contains(output, canary)
		}
	}
	return counts, leaked
}

func apiKeyDerived(t *testing.T, st *store.Store) []string {
	t.Helper()
	rows := apiKeyRows(t, st, "select f.ts || '|' || f.file_id || '|' || f.name || '|' || m.target_id as record from message_files f join message_mentions m on f.channel_id=m.channel_id and f.ts=m.ts where f.deleted_at is null and m.deleted_at is null and m.mention_type='user' order by f.ts")
	require.Len(t, apiKeyRows(t, st, "select * from message_files"), len(rows))
	require.Len(t, apiKeyRows(t, st, "select * from message_mentions"), len(rows))
	records := make([]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, row["record"].(string))
	}
	return records
}
