package slackapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestRepairMessageKeyAdmission(t *testing.T) {
	for _, tc := range []struct{ name, endpoint, timestamp string }{
		{"history-empty", "history", ""},
		{"replies-whitespace", "replies", " \t "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer st.Close()
			now := time.Unix(1710000200, 0).UTC()
			const parentTS = "1710000001.000000"
			const priorLatest = "1709900000.000000"
			const attemptedOldest = "1709896400.000000"
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, saveHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "", historyCoverage{Complete: true, Latest: priorLatest}))
			_, err := st.DB().ExecContext(ctx, "insert into sync_state(source_name,entity_type,entity_id,value,updated_at) values(?,?,?,?,?)", SourceBot, "workspace", "T123", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			parent := slack.Message{Msg: slack.Msg{Channel: "C123", Type: "message", Timestamp: parentTS, Text: "history-parent", ReplyCount: 2}}
			if tc.endpoint == "replies" {
				require.NoError(t, st.UpsertMessage(ctx, toStoreMessage("T123", parent, SourceBot, 2, nil, now), nil))
			}
			beforeWorkspace := repairKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'")
			beforeParent := repairKeyParent(t, st)
			var corrected atomic.Bool
			var mu sync.Mutex
			var calls, oldest []string
			server := admissionServer(t, []map[string]any{{"id": "C123", "name": "fixture", "is_channel": true}}, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/conversations.history" && r.URL.Path != "/conversations.replies" {
					return false
				}
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, r.URL.Path+":"+r.Form.Get("cursor")+":"+r.Form.Get("ts"))
				require.Equal(t, "C123", r.Form.Get("channel"))
				payload := map[string]any{"ok": true}
				badTS := tc.timestamp
				if corrected.Load() {
					badTS = "1710000004.000000"
				}
				bad := repairKeyMessage("message-key-bad", badTS)
				if r.URL.Path == "/conversations.history" {
					oldest = append(oldest, r.Form.Get("oldest"))
					require.Equal(t, "1710000200.000000", r.Form.Get("latest"))
					if tc.endpoint == "replies" {
						payload["messages"] = []any{parent}
					} else if r.Form.Get("cursor") == "" {
						payload["messages"] = []any{repairKeyMessage("earlier-page", "1710000000.000000")}
						payload["response_metadata"] = map[string]any{"next_cursor": "second"}
					} else {
						require.Equal(t, "second", r.Form.Get("cursor"))
						sibling := repairKeyMessage("message-key-sibling", "1710000003.000000")
						sibling["reply_count"] = 1
						payload["messages"] = []any{sibling, bad}
					}
				} else if tc.endpoint == "history" {
					// Permit the old parent's premature request so its baseline
					// failure is the missing admission guard, not an HTTP fixture gap.
					require.Equal(t, "1710000003.000000", r.Form.Get("ts"))
					payload["messages"] = []any{}
				} else if r.Form.Get("cursor") == "" {
					require.Equal(t, parentTS, r.Form.Get("ts"))
					reply := repairKeyMessage("earlier-reply", "1710000002.000000")
					reply["thread_ts"] = parentTS
					payload["messages"] = []any{reply}
					payload["response_metadata"] = map[string]any{"next_cursor": "second"}
				} else {
					require.Equal(t, "second", r.Form.Get("cursor"))
					require.Equal(t, parentTS, r.Form.Get("ts"))
					echo := parent
					echo.Text, echo.ThreadTimestamp = "reply-parent-replacement", parentTS
					sibling := repairKeyMessage("message-key-sibling", "1710000003.000000")
					sibling["thread_ts"] = parentTS
					bad["thread_ts"] = parentTS
					payload["messages"] = []any{echo, sibling, bad}
				}
				writeAdmissionJSON(t, w, payload)
				return true
			})
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			client.now = func() time.Time { return now }
			runErr := client.repairWorkspace(ctx, st, "T123")
			coverage, err := loadHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			coverageJSON, err := json.Marshal(coverage)
			require.NoError(t, err)
			rows := repairKeyRows(t, st, "select ts,source_name,source_rank from messages order by ts")
			badRows := repairKeyRows(t, st, "select ts from messages where text like 'message-key-bad%'")
			parentPreserved := reflect.DeepEqual(beforeParent, repairKeyParent(t, st))
			workspacePreserved := reflect.DeepEqual(beforeWorkspace, repairKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			events := repairKeyRows(t, st, "select * from message_events")
			files := repairKeyRows(t, st, "select * from message_files")
			mentions := repairKeyRows(t, st, "select * from message_mentions")
			mu.Lock()
			observedCalls, observedOldest := append([]string(nil), calls...), append([]string(nil), oldest...)
			mu.Unlock()
			t.Logf("repair-key observation: error=%t coverage=%s rows=%d events=%d files=%d mentions=%d parent_preserved=%t workspace_preserved=%t requests=%v oldest=%v", runErr != nil, coverageJSON, len(rows), len(events), len(files), len(mentions), parentPreserved, workspacePreserved, observedCalls, observedOldest)
			expectedError := "channel C123 " + tc.endpoint + ": message is missing a timestamp"
			if runErr == nil || runErr.Error() != expectedError || coverage.Pending == nil || len(badRows) != 0 || !parentPreserved || !workspacePreserved {
				t.Fatalf("repair-key boundary: error=%t expected_error=%t pending=%t workspace_preserved=%t bad_rows=%d parent_preserved=%t", runErr != nil, runErr != nil && runErr.Error() == expectedError, coverage.Pending != nil, workspacePreserved, len(badRows), parentPreserved)
			}
			require.Equal(t, historyCoverage{Complete: true, Latest: priorLatest, Pending: new(attemptedOldest)}, coverage)
			assertAdmissionCanariesAbsent(t, st, fmt.Sprint(runErr), "message-key-bad", "message-key-sibling", "reply-parent-replacement", "UMESSAGEKEYBAD", "UMESSAGEKEYSIBLING", "FMESSAGEKEYBAD", "FMESSAGEKEYSIBLING")
			if tc.endpoint == "history" {
				require.Len(t, rows, 1)
				require.Equal(t, "1710000000.000000", rows[0]["ts"])
				require.Zero(t, server.calls("conversations.replies"))
				require.Equal(t, []string{"1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE"}, repairKeyDerived(t, st))
			} else {
				require.Len(t, rows, 2)
				require.Equal(t, "1710000002.000000", rows[1]["ts"])
				require.Equal(t, SourceUser, rows[1]["source_name"])
				require.EqualValues(t, 1, rows[1]["source_rank"])
				require.Equal(t, 2, server.calls("conversations.replies"))
				require.Equal(t, []string{"1710000002.000000|FEARLIERREPLY|earlier-reply.txt|UEARLIERREPLY"}, repairKeyDerived(t, st))
			}
			require.Len(t, events, len(rows))
			require.Len(t, repairKeyRows(t, st, "select * from message_event_heads"), len(rows))
			require.Len(t, repairKeyRows(t, st, "select * from message_fts"), len(rows))
			corrected.Store(true)
			require.NoError(t, client.repairWorkspace(ctx, st, "T123"))
			coverage, err = loadHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			require.Equal(t, historyCoverage{Complete: true, Latest: "1710000200.000000"}, coverage)
			mu.Lock()
			observedOldest = append([]string(nil), oldest...)
			mu.Unlock()
			wantHistoryCalls := 2
			if tc.endpoint == "history" {
				wantHistoryCalls = 4
			}
			require.Len(t, observedOldest, wantHistoryCalls)
			for _, value := range observedOldest {
				require.Equal(t, attemptedOldest, value, "retry keeps the unfinished interval")
			}
			wantRows, wantEvents := 3, 3
			firstDerived := "1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE"
			if tc.endpoint == "replies" {
				wantRows, wantEvents = 4, 5
				firstDerived = "1710000002.000000|FEARLIERREPLY|earlier-reply.txt|UEARLIERREPLY"
			}
			require.Len(t, repairKeyRows(t, st, "select ts from messages"), wantRows)
			require.Len(t, repairKeyRows(t, st, "select * from message_events"), wantEvents)
			require.Len(t, repairKeyRows(t, st, "select * from message_event_heads"), wantEvents)
			require.Len(t, repairKeyRows(t, st, "select * from message_fts"), wantRows)
			require.Equal(t, []string{firstDerived, "1710000003.000000|FMESSAGEKEYSIBLING|message-key-sibling.txt|UMESSAGEKEYSIBLING", "1710000004.000000|FMESSAGEKEYBAD|message-key-bad.txt|UMESSAGEKEYBAD"}, repairKeyDerived(t, st))
			// repairWorkspace does not publish the ordinary API workspace marker.
			require.Equal(t, beforeWorkspace, repairKeyRows(t, st, "select * from sync_state where source_name='api-bot' and entity_type='workspace'"))
			if tc.endpoint == "replies" {
				parentRows := repairKeyRows(t, st, "select source_name,source_rank,text from messages where ts='1710000001.000000'")
				require.Equal(t, SourceUser, parentRows[0]["source_name"])
				require.EqualValues(t, 1, parentRows[0]["source_rank"])
				require.Equal(t, "reply-parent-replacement", parentRows[0]["text"])
			}
		})
	}
}

func repairKeyRows(t *testing.T, st *store.Store, query string) []map[string]any {
	t.Helper()
	rows, err := st.QueryReadOnly(context.Background(), query)
	require.NoError(t, err, query)
	return rows
}

func repairKeyParent(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	return map[string][]map[string]any{
		"message": repairKeyRows(t, st, "select ts,source_name,source_rank,text,normalized_text,reply_count,thread_ts,raw_json from messages where ts='1710000001.000000'"),
		"events":  repairKeyRows(t, st, "select * from message_events where ts='1710000001.000000' order by id"),
		"heads":   repairKeyRows(t, st, "select * from message_event_heads where ts='1710000001.000000' order by channel_id,ts,event_type,source_name"),
	}
}

func repairKeyMessage(text, ts string) map[string]any {
	message := admissionMessage(text, "C123", ts)
	suffix := strings.ToUpper(strings.ReplaceAll(text, "-", ""))
	message["text"] = text + " <@U" + suffix + ">"
	message["files"] = []any{map[string]any{"id": "F" + suffix, "name": text + ".txt", "title": text}}
	return message
}

func repairKeyDerived(t *testing.T, st *store.Store) []string {
	t.Helper()
	rows := repairKeyRows(t, st, "select f.ts || '|' || f.file_id || '|' || f.name || '|' || m.target_id as record from message_files f join message_mentions m on f.channel_id=m.channel_id and f.ts=m.ts where f.deleted_at is null and m.deleted_at is null and m.mention_type='user' order by f.ts")
	require.Len(t, repairKeyRows(t, st, "select * from message_files"), len(rows))
	require.Len(t, repairKeyRows(t, st, "select * from message_mentions"), len(rows))
	records := make([]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, row["record"].(string))
	}
	return records
}
