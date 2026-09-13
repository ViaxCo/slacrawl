package slackapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestCompletenessDecoderPreservesCapabilityProbes(t *testing.T) {
	for _, method := range []string{"history", "replies"} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				require.NoError(t, r.ParseForm())
				require.Equal(t, "1", r.Form.Get("limit"))
				require.Equal(t, "/conversations."+method, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{}, "has_more": true, "is_limited": true})
			}))
			defer server.Close()
			client := NewWithOptions(config.Tokens{User: "fixture"}, server.URL+"/", server.Client())
			if method == "history" {
				// Doctor suppresses non-scope probe failures, so exercise the
				// decoder directly to catch an incorrectly placed completion gate.
				page, err := client.getConversationHistory(context.Background(), "fixture", &slack.GetConversationHistoryParameters{ChannelID: "C123", Limit: 1})
				require.NoError(t, err)
				require.True(t, page.HasMore)
				require.True(t, page.IsLimited)
				require.Empty(t, page.NextCursor)
			} else {
				page, err := client.getConversationReplies(context.Background(), &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000001.000000", Limit: 1})
				require.NoError(t, err)
				require.True(t, page.HasMore)
				require.Empty(t, page.NextCursor)
			}
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestHistoryCompletenessAcrossSources(t *testing.T) {
	for _, sourceName := range []string{SourceBot, SourceUser} {
		for _, mode := range []string{"more", "limited", "unknown-limited"} {
			t.Run(sourceName+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				st := mustStore(t)
				defer st.Close()
				now := time.Unix(1710000200, 0).UTC()
				channel := slack.Channel{GroupConversation: slack.GroupConversation{Conversation: slack.Conversation{ID: "C123"}}}
				require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", UpdatedAt: now}))
				prior := historyCoverage{Complete: true, Latest: "1709900000.000000"}
				oldest := "1709896400.000000"
				if mode == "unknown-limited" {
					prior = historyCoverage{}
					oldest = ""
				} else {
					require.NoError(t, saveHistoryCoverage(ctx, st, sourceName, "T123", "C123", "", prior))
				}
				var cursorsMu sync.Mutex
				var cursors []string
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.NoError(t, r.ParseForm())
					require.Equal(t, sourceName, r.Form.Get("token"))
					require.Equal(t, oldest, r.Form.Get("oldest"))
					require.Equal(t, "1710000200.000000", r.Form.Get("latest"))
					cursor := r.Form.Get("cursor")
					cursorsMu.Lock()
					cursors = append(cursors, cursor)
					cursorsMu.Unlock()
					payload := map[string]any{"ok": true, "messages": []any{map[string]any{"channel": "C123", "ts": "1710000000.000000", "text": "first"}}}
					if cursor == "" {
						payload["is_limited"] = mode != "more"
						payload["response_metadata"] = map[string]any{"next_cursor": "second"}
					} else {
						require.Equal(t, "second", cursor)
						payload["messages"] = []any{map[string]any{"channel": "C123", "ts": "1710000001.000000", "text": "second"}}
						payload["has_more"] = mode == "more"
						payload["is_limited"] = false
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(payload)
				}))
				defer server.Close()
				client := NewWithOptions(config.Tokens{Bot: SourceBot, User: SourceUser}, server.URL+"/", server.Client())
				source := channelSyncSource{historyClient: client.bot, token: sourceName, sourceName: sourceName, sourceRank: 2}
				if sourceName == SourceUser {
					source.historyClient, source.sourceRank = client.user, 1
				}
				err := client.syncChannelMessagesWithSource(ctx, st, "T123", channel, oldest, false, now, false, source)
				if mode == "more" {
					require.ErrorContains(t, err, "has_more without a continuation cursor")
				} else {
					require.ErrorContains(t, err, "completeness of the requested interval is uncertified")
				}
				cursorsMu.Lock()
				gotCursors := append([]string(nil), cursors...)
				cursorsMu.Unlock()
				require.Equal(t, []string{"", "second"}, gotCursors)
				coverage, err := loadHistoryCoverage(ctx, st, sourceName, "T123", "C123", "")
				require.NoError(t, err)
				require.Equal(t, prior.Latest, coverage.Latest)
				require.Equal(t, prior.Complete, coverage.Complete)
				require.NotNil(t, coverage.Pending)
				require.Equal(t, oldest, *coverage.Pending)
				rows, err := st.QueryReadOnly(ctx, "select source_name,source_rank from messages order by ts")
				require.NoError(t, err)
				require.Len(t, rows, 2, "valid bounded pages remain committed")
				for _, row := range rows {
					require.Equal(t, sourceName, row["source_name"])
					require.EqualValues(t, source.sourceRank, row["source_rank"])
				}
			})
		}
	}
}

func TestHistoryCompletenessKeepsConcreteFailures(t *testing.T) {
	for _, mode := range []string{"request", "decode", "identity", "timestamp", "store", "thread", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := mustStore(t)
			defer st.Close()
			now := time.Unix(1710000200, 0).UTC()
			channel := slack.Channel{GroupConversation: slack.GroupConversation{Conversation: slack.Conversation{ID: "C123"}}}
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", UpdatedAt: now}))
			require.NoError(t, saveHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "", historyCoverage{Complete: true, Latest: "1709900000.000000"}))
			if mode == "store" {
				_, err := st.DB().ExecContext(ctx, "create trigger reject_second before insert on messages when new.ts='1710000001.000000' begin select raise(abort,'synthetic_write_failure'); end")
				require.NoError(t, err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, r.ParseForm())
				payload := map[string]any{"ok": true}
				if r.URL.Path == "/conversations.replies" {
					require.Equal(t, "thread", mode)
					payload["ok"], payload["error"] = false, "synthetic_thread_failure"
				} else if r.Form.Get("cursor") == "" {
					payload["messages"] = []any{map[string]any{"channel": "C123", "ts": "1710000000.000000", "text": "first"}}
					payload["is_limited"] = true
					payload["response_metadata"] = map[string]any{"next_cursor": "second"}
				} else {
					message := map[string]any{"channel": "C123", "ts": "1710000001.000000", "text": "second"}
					payload["messages"], payload["has_more"] = []any{message}, true
					switch mode {
					case "request":
						payload["ok"], payload["error"] = false, "synthetic_request_failure"
					case "decode":
						payload["messages"] = []any{123}
					case "identity":
						message["channel"] = "COTHER"
					case "timestamp":
						message["ts"] = ""
					case "thread":
						message["reply_count"] = 1
					case "cancellation":
						cancel()
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(payload)
			}))
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture", User: "fixture-user"}, server.URL+"/", server.Client())
			source := channelSyncSource{historyClient: client.bot, token: "fixture", sourceName: SourceBot, sourceRank: 2}
			err := client.syncChannelMessagesWithSource(ctx, st, "T123", channel, "1709896400.000000", false, now, true, source)
			if mode == "cancellation" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				want := map[string]string{"request": "synthetic_request_failure", "decode": "cannot unmarshal number", "identity": "message channel does not match requested conversation", "timestamp": "message is missing a timestamp", "store": "synthetic_write_failure", "thread": "synthetic_thread_failure"}
				require.ErrorContains(t, err, want[mode])
			}
			require.NotContains(t, err.Error(), "continuation cursor")
			require.NotContains(t, err.Error(), "uncertified")
			coverage, err := loadHistoryCoverage(context.Background(), st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			require.True(t, coverage.Complete)
			require.Equal(t, "1709900000.000000", coverage.Latest)
			require.NotNil(t, coverage.Pending)
			require.Equal(t, "1709896400.000000", *coverage.Pending)
			rows, err := st.QueryReadOnly(context.Background(), "select ts from messages order by ts")
			require.NoError(t, err)
			wantRows := 1
			if mode == "thread" {
				wantRows = 2
			}
			require.Len(t, rows, wantRows)
			require.Equal(t, "1710000000.000000", rows[0]["ts"])
		})
	}
}

func TestRepairCompletenessRetriesPendingInterval(t *testing.T) {
	for _, mode := range []string{"history-more", "replies-more", "limited-earlier"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer st.Close()
			now := time.Unix(1710000200, 0).UTC()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", UpdatedAt: now}))
			require.NoError(t, saveHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "", historyCoverage{Complete: true, Latest: "1709900000.000000"}))
			require.NoError(t, st.SetSyncState(ctx, SourceBot, "workspace", "T123", "2020-01-01T00:00:00Z"))
			beforeWorkspace, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-bot' and entity_type='workspace'")
			require.NoError(t, err)
			var corrected atomic.Bool
			var startsMu sync.Mutex
			var starts []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, r.ParseForm())
				payload := map[string]any{"ok": true}
				switch r.URL.Path {
				case "/auth.test":
					payload["team_id"] = "T123"
				case "/conversations.list":
					payload["channels"] = []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true}}
				case "/conversations.history":
					if r.Form.Get("cursor") == "" {
						startsMu.Lock()
						starts = append(starts, r.Form.Get("oldest"))
						startsMu.Unlock()
						payload["messages"] = []any{map[string]any{"channel": "C123", "ts": "1710000000.000000", "text": "parent", "reply_count": 1}}
						payload["response_metadata"] = map[string]any{"next_cursor": "second"}
						payload["is_limited"] = mode == "limited-earlier" && !corrected.Load()
					} else {
						payload["messages"] = []any{map[string]any{"channel": "C123", "ts": "1710000002.000000", "text": "older page"}}
						payload["has_more"] = mode == "history-more" && !corrected.Load()
					}
				case "/conversations.replies":
					payload["messages"] = []any{map[string]any{"channel": "C123", "ts": "1710000001.000000", "thread_ts": "1710000000.000000", "text": "reply"}}
					payload["has_more"] = mode == "replies-more" && !corrected.Load()
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(payload)
			}))
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture", User: "fixture-user"}, server.URL+"/", server.Client())
			client.now = func() time.Time { return now }
			wantError := "has_more without a continuation cursor"
			if mode == "limited-earlier" {
				wantError = "completeness of the requested interval is uncertified"
			}
			require.ErrorContains(t, client.repairWorkspace(ctx, st, "T123"), wantError)
			coverage, err := loadHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			require.True(t, coverage.Complete)
			require.Equal(t, "1709900000.000000", coverage.Latest)
			require.NotNil(t, coverage.Pending)
			require.Equal(t, "1709896400.000000", *coverage.Pending)
			rows, err := st.QueryReadOnly(ctx, "select ts from messages order by ts")
			require.NoError(t, err)
			wantRows := 3
			if mode == "replies-more" {
				wantRows = 2
			}
			require.Len(t, rows, wantRows)
			corrected.Store(true)
			require.NoError(t, client.repairWorkspace(ctx, st, "T123"))
			startsMu.Lock()
			gotStarts := append([]string(nil), starts...)
			startsMu.Unlock()
			require.Equal(t, []string{"1709896400.000000", "1709896400.000000"}, gotStarts)
			coverage, err = loadHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			require.True(t, coverage.Complete)
			require.Nil(t, coverage.Pending)
			require.Equal(t, "1710000200.000000", coverage.Latest)
			rows, err = st.QueryReadOnly(ctx, "select ts from messages order by ts")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"ts": "1710000000.000000"}, {"ts": "1710000001.000000"}, {"ts": "1710000002.000000"}}, rows)
			afterWorkspace, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-bot' and entity_type='workspace'")
			require.NoError(t, err)
			require.Equal(t, beforeWorkspace, afterWorkspace, "periodic repair does not publish the ordinary workspace success marker")
		})
	}
}
