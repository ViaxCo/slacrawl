package slackapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestAPIExcludesDMsBeforePersistence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		repair      bool
		concurrency int
	}{
		{"sync", false, 1}, {"concurrent sync", false, 2}, {"repair", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channels := []map[string]any{
				{"id": "CPUBLIC", "is_channel": true},
				{"id": "CPRIVATE", "is_channel": true, "is_private": true},
				{"id": "GLEGACY", "is_group": true, "is_private": true},
			}
			for _, flags := range []map[string]any{
				{"is_im": true}, {"is_mpim": true, "is_private": true},
				{"is_im": true, "is_channel": true}, {"is_mpim": true, "is_group": true},
			} {
				channel := maps.Clone(flags)
				channel["id"] = fmt.Sprintf("CFORBIDDEN%d", len(channels))
				channel["name"] = "forbidden-canary"
				// Excluded content need not be inspected, even when its nested
				// identity would be rejected on a retained conversation.
				channel["latest"] = admissionMessage("forbidden-canary", "DOTHER", "1710000001.000000")
				channels = append(channels, channel)
			}
			server := admissionServer(t, channels, nil)
			defer server.Close()
			var logs bytes.Buffer
			client := NewWithOptions(config.Tokens{Bot: "fixture", User: "fixture-user"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude).WithLogger(testProgressLogger(&logs))
			st := mustStore(t)
			defer st.Close()
			var err error
			if tc.repair {
				err = client.repairWorkspace(context.Background(), st, "T123")
			} else {
				err = client.Sync(context.Background(), st, SyncOptions{Concurrency: tc.concurrency})
			}
			require.NoError(t, err)
			require.Equal(t, 1, server.calls("conversations.list"), "no separate DM discovery")
			require.Equal(t, 3, server.calls("conversations.history"))
			require.Zero(t, server.calls("conversations.replies"))
			rows, err := st.QueryReadOnly(context.Background(), "select id from channels order by id")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"id": "CPRIVATE"}, {"id": "CPUBLIC"}, {"id": "GLEGACY"}}, rows)
			for _, table := range []string{"messages", "message_events", "message_event_heads", "message_files", "message_mentions", "message_fts"} {
				rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
				require.NoError(t, err)
				require.NotEmpty(t, rows, table+" proves admitted messages still work")
			}
			assertAdmissionCanariesAbsent(t, st, logs.String(), "forbidden-canary", "CFORBIDDEN", "DOTHER")
		})
	}
}

func TestAPIConversationTypesAndSelection(t *testing.T) {
	for _, flags := range []map[string]any{
		{}, {"is_private": true}, {"is_group": true},
		{"is_channel": true, "is_group": true},
		{"is_channel": true, "is_group": true, "is_private": true},
	} {
		for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
			t.Run(fmt.Sprintf("flags=%v/policy=%d", flags, policy), func(t *testing.T) {
				unknown := maps.Clone(flags)
				unknown["id"], unknown["name"] = "CUNKNOWN", "unknown-canary"
				server := admissionServer(t, []map[string]any{{"id": "CPUBLIC", "is_channel": true}, unknown}, nil)
				defer server.Close()
				var logs bytes.Buffer
				client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(policy).WithLogger(testProgressLogger(&logs))
				st := mustStore(t)
				defer st.Close()
				err := client.Sync(context.Background(), st, SyncOptions{Concurrency: 2})
				if policy == admission.Exclude {
					require.ErrorContains(t, err, "unknown or conflicting conversation type")
					require.NotContains(t, err.Error(), "unknown-canary")
					require.Zero(t, server.calls("conversations.history"))
					require.Empty(t, logs.String(), "admission precedes progress")
					assertAdmissionDataEmpty(t, st)
				} else {
					require.NoError(t, err, "omitted/true retain existing type defaults")
					require.Equal(t, 2, server.calls("conversations.history"))
				}
			})
		}
	}
	for _, opts := range []SyncOptions{{Channels: []string{"CPUBLIC"}}, {ExcludeChannels: []string{"unknown-canary"}}} {
		t.Run(fmt.Sprint(opts), func(t *testing.T) {
			// Invalid, unselected metadata must not widen an explicit sync scope.
			server := admissionServer(t, []map[string]any{{"id": "CPUBLIC", "is_channel": true}, {"id": "CUNKNOWN", "name": "unknown-canary", "context_team_id": "TOTHER"}}, nil)
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			st := mustStore(t)
			defer st.Close()
			require.NoError(t, client.Sync(context.Background(), st, opts))
			require.Equal(t, 1, server.calls("conversations.history"))
			assertAdmissionCanariesAbsent(t, st, "", "unknown-canary", "CUNKNOWN", "TOTHER")
		})
	}
}

func TestAPIRejectsConversationIdentityBeforePersistence(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, field := range []string{"id", "context_team_id", "channel", "message", "previous_message", "root"} {
			t.Run(fmt.Sprintf("policy=%d/field=%s", policy, field), func(t *testing.T) {
				channel := map[string]any{"id": "C123", "is_channel": true, "name": "identity-canary"}
				switch field {
				case "id":
					channel["id"] = ""
				case "context_team_id":
					channel[field] = "TOTHER"
				default:
					latest := admissionMessage("identity-canary", "C123", "1710000000.000000")
					conflictMessageChannel(latest, field)
					channel["latest"] = latest
				}
				server := admissionServer(t, []map[string]any{channel}, nil)
				defer server.Close()
				var logs bytes.Buffer
				client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(policy).WithLogger(testProgressLogger(&logs))
				st := mustStore(t)
				defer st.Close()
				err := client.Sync(context.Background(), st, SyncOptions{})
				require.Error(t, err)
				require.NotContains(t, err.Error(), "identity-canary")
				require.Zero(t, server.calls("conversations.history"))
				require.Empty(t, logs.String())
				assertAdmissionDataEmpty(t, st)
			})
		}
	}
}

func TestAPIRejectsWholeMessagePageBeforePersistence(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, endpoint := range []string{"conversations.history", "conversations.replies"} {
			for _, field := range []string{"channel", "message", "previous_message", "root"} {
				t.Run(fmt.Sprintf("policy=%d/%s/%s", policy, endpoint, field), func(t *testing.T) {
					server := admissionServer(t, []map[string]any{{"id": "C123", "is_channel": true}}, func(w http.ResponseWriter, r *http.Request) bool {
						if r.URL.Path == "/conversations.history" && endpoint == "conversations.replies" {
							writeAdmissionJSON(t, w, map[string]any{"ok": true, "messages": []any{map[string]any{"type": "message", "ts": "1710000000.000000", "reply_count": 1}}})
							return true
						}
						if r.URL.Path != "/"+endpoint {
							return false
						}
						if r.Form.Get("cursor") == "" {
							writeAdmissionJSON(t, w, map[string]any{"ok": true, "messages": []any{admissionMessage("earlier-page", "C123", "1710000001.000000")}, "response_metadata": map[string]any{"next_cursor": "second"}})
						} else {
							bad := admissionMessage("rejected-page-canary", "C123", "1710000003.000000")
							conflictMessageChannel(bad, field)
							if field == "previous_message" {
								bad["subtype"] = "message_deleted"
								bad["ts"] = "1710000001.000000"
								bad["deleted_ts"] = "1710000003.000000"
							}
							samePage := admissionMessage("same-page-canary", "C123", "1710000002.000000")
							samePage["reply_count"] = 1
							writeAdmissionJSON(t, w, map[string]any{"ok": true, "messages": []any{samePage, bad}})
						}
						return true
					})
					defer server.Close()
					var logs bytes.Buffer
					client := NewWithOptions(config.Tokens{Bot: "fixture", User: "fixture-user"}, server.URL()+"/", server.Client()).WithDMPolicy(policy).WithLogger(testProgressLogger(&logs))
					client.now = func() time.Time { return time.Unix(1710000200, 0).UTC() }
					st := mustStore(t)
					defer st.Close()
					oldLatest := "1709900000.000000"
					require.NoError(t, seedAPIHistory(context.Background(), st, SourceBot, "T123", "C123", "", store.APIHistoryState{Complete: true, Latest: oldLatest}))
					err := client.Sync(context.Background(), st, SyncOptions{})
					require.ErrorContains(t, err, "message channel does not match requested conversation")
					if endpoint == "conversations.history" {
						require.Zero(t, server.calls("conversations.replies"), "rejected pages cannot launch thread work")
					}
					assertAdmissionCanariesAbsent(t, st, logs.String()+err.Error(), "rejected-page-canary", "same-page-canary", "CFOREIGN")
					rows, err := st.SearchMessages(context.Background(), store.SearchOptions{Query: "earlier", Mode: store.SearchModeRawFTS, Limit: 10})
					require.NoError(t, err)
					require.Len(t, rows, 1, "the previously committed page survives")
					coverage, err := readAPIHistory(context.Background(), st, SourceBot, "T123", "C123", "")
					require.NoError(t, err)
					require.Equal(t, oldLatest, coverage.Latest)
					require.NotNil(t, coverage.Pending)
					require.Contains(t, logs.String(), "state=failed")
					require.NotContains(t, logs.String(), "state=finished")
					checkpoints, err := st.QueryReadOnly(context.Background(), "select * from sync_state where source_name = 'api-bot' and entity_type = 'workspace'")
					require.NoError(t, err)
					require.Empty(t, checkpoints, "failure cannot publish a workspace success checkpoint")
				})
			}
		}
	}
}

func TestAPIAcceptsSharedMessageIdentity(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			message := admissionMessage("shared", "", "1710000000.000000")
			message["team"] = "TEXTERNAL"
			message["source_team"] = "TEXTERNAL"
			message["user_team"] = "TEXTERNAL"
			for _, field := range []string{"message", "previous_message", "root"} {
				message[field] = map[string]any{"channel": "C123", "team": "TEXTERNAL", "ts": "1700000000.000000"}
			}
			server := admissionServer(t, []map[string]any{{"id": "C123", "is_channel": true, "is_shared": true, "context_team_id": "T123", "conversation_host_id": "TEXTERNAL", "latest": message}}, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/conversations.history" {
					return false
				}
				writeAdmissionJSON(t, w, map[string]any{"ok": true, "messages": []any{message}})
				return true
			})
			defer server.Close()
			st := mustStore(t)
			defer st.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(policy)
			require.NoError(t, client.Sync(context.Background(), st, SyncOptions{}))
			rows, err := st.QueryReadOnly(context.Background(), "select channel_id, workspace_id from messages")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"channel_id": "C123", "workspace_id": "T123"}}, rows)
		})
	}
}

func admissionMessage(text, channelID, ts string) map[string]any {
	return map[string]any{
		"type": "message", "channel": channelID, "ts": ts, "text": text + " <@U123>",
		"blocks":      []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": text}}},
		"attachments": []any{map[string]any{"fallback": text}},
		"files":       []any{map[string]any{"id": "F123", "title": text, "name": text + ".txt"}},
	}
}

func conflictMessageChannel(message map[string]any, field string) {
	if field == "channel" {
		message[field] = "CFOREIGN"
	} else {
		message[field] = admissionMessage("rejected-page-canary", "CFOREIGN", "1700000000.000000")
	}
}

func admissionServer(t *testing.T, channels []map[string]any, handle func(http.ResponseWriter, *http.Request) bool) *mockSlackServer {
	t.Helper()
	mock := &mockSlackServer{counts: map[string]int{}}
	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		mock.counts[r.URL.Path]++
		mock.mu.Unlock()
		require.NoError(t, r.ParseForm())
		w.Header().Set("Content-Type", "application/json")
		if handle != nil && handle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/auth.test":
			writeAdmissionJSON(t, w, map[string]any{"ok": true, "team_id": "T123", "team": "Fixture"})
		case "/conversations.list":
			if strings.Contains(r.Form.Get("types"), "im") {
				writeAdmissionJSON(t, w, map[string]any{"ok": true, "channels": []any{}})
			} else {
				writeAdmissionJSON(t, w, map[string]any{"ok": true, "channels": channels})
			}
		case "/conversations.history":
			writeAdmissionJSON(t, w, map[string]any{"ok": true, "messages": []any{admissionMessage("admitted", r.Form.Get("channel"), "1710000000.000000")}})
		case "/users.list":
			writeAdmissionJSON(t, w, map[string]any{"ok": true, "members": []any{}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	return mock
}

func writeAdmissionJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

var admissionTables = []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "sync_state", "message_mentions", "embedding_jobs", "message_fts"}

func assertAdmissionCanariesAbsent(t *testing.T, st *store.Store, logs string, canaries ...string) {
	t.Helper()
	for _, table := range admissionTables {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		payload, err := json.Marshal(rows)
		require.NoError(t, err)
		for _, canary := range canaries {
			require.NotContains(t, string(payload), canary, table)
			require.NotContains(t, logs, canary)
		}
	}
}

func assertAdmissionDataEmpty(t *testing.T, st *store.Store) {
	t.Helper()
	for _, table := range admissionTables {
		if table == "workspaces" {
			continue // Authenticated workspace metadata precedes channel discovery.
		}
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		require.Empty(t, rows, table)
	}
}
