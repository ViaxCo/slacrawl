package slackapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestConversationPagesRequireExplicitSuccess(t *testing.T) {
	for _, method := range []string{"history", "replies"} {
		for _, status := range []struct{ name, field string }{{"missing", ""}, {"null", `"ok":null,`}, {"false", `"ok":false,`}} {
			for _, detail := range []struct{ name, field string }{{"missing", ""}, {"null", `"error":null,`}, {"empty", `"error":"",`}, {"blank", `"error":" \t ",`}} {
				t.Run(method+"/"+status.name+"/"+detail.name, func(t *testing.T) {
					calls := 0
					client := primaryOwnerClient(t, config.Tokens{User: "fixture"}, func(r *http.Request, form url.Values) (any, error) {
						calls++
						require.Equal(t, "/conversations."+method, r.URL.Path)
						require.Equal(t, "fixture", form.Get("token"))
						return json.RawMessage(`{` + status.field + detail.field + `"messages":[{"ts":"1710000000.000000","text":"rejected-page-canary"}]}`), nil
					})
					var err error
					if method == "history" {
						var page *conversationHistoryPage
						page, err = client.getConversationHistory(context.Background(), "fixture", &slack.GetConversationHistoryParameters{ChannelID: "C123"})
						require.Nil(t, page)
					} else {
						var page *conversationRepliesPage
						page, err = client.getConversationReplies(context.Background(), &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000000.000000"})
						require.Nil(t, page)
					}
					require.EqualError(t, err, "conversations."+method+" response did not report success")
					require.NotContains(t, err.Error(), "rejected-page-canary")
					require.Equal(t, 1, calls, "a malformed success response is not rate-limited")
				})
			}
		}
		t.Run(method+"/before-message-conversion", func(t *testing.T) {
			client := primaryOwnerClient(t, config.Tokens{User: "fixture"}, func(r *http.Request, _ url.Values) (any, error) {
				require.Equal(t, "/conversations."+method, r.URL.Path)
				return json.RawMessage(`{"ok":false,"messages":[42]}`), nil
			})
			if method == "history" {
				page, err := client.getConversationHistory(context.Background(), "fixture", &slack.GetConversationHistoryParameters{ChannelID: "C123"})
				require.Nil(t, page)
				require.EqualError(t, err, "conversations.history response did not report success")
			} else {
				page, err := client.getConversationReplies(context.Background(), &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000000.000000"})
				require.Nil(t, page)
				require.EqualError(t, err, "conversations.replies response did not report success")
			}
		})
	}
}

func TestConversationPageSuccessPreservesNativeErrors(t *testing.T) {
	response := slack.SlackResponse{
		Error: "missing_scope", Errors: []slack.SlackResponseErrors{{Message: new("native-detail")}},
		ResponseMetadata: slack.ResponseMetadata{Messages: []string{"native-message"}, Warnings: []string{"native-warning"}, Cursor: "native-cursor"},
	}
	// Direct helper input proves metadata passthrough. Page decoding's existing
	// response_metadata cursor field does not populate the embedded metadata.
	require.Equal(t, response.Err(), conversationPageSuccess("conversations.history", response))
	for _, method := range []string{"history", "replies"} {
		t.Run(method, func(t *testing.T) {
			client := primaryOwnerClient(t, config.Tokens{User: "fixture"}, func(r *http.Request, _ url.Values) (any, error) {
				require.Equal(t, "/conversations."+method, r.URL.Path)
				return json.RawMessage(`{"ok":false,"error":"missing_scope","errors":["native-detail"],"messages":[42]}`), nil
			})
			var err error
			if method == "history" {
				var page *conversationHistoryPage
				page, err = client.getConversationHistory(context.Background(), "fixture", &slack.GetConversationHistoryParameters{ChannelID: "C123"})
				require.Nil(t, page)
			} else {
				var page *conversationRepliesPage
				page, err = client.getConversationReplies(context.Background(), &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000000.000000"})
				require.Nil(t, page)
			}
			var native slack.SlackErrorResponse
			require.ErrorAs(t, err, &native)
			require.Equal(t, "missing_scope", native.Err)
			require.Equal(t, response.Errors, native.Errors)
		})
	}
}

func TestConversationPageRateLimitRetryAndCancellation(t *testing.T) {
	for _, method := range []string{"history", "replies"} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/canceled=%t", method, canceled), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls, sleeps := 0, 0
				client := NewWithOptions(config.Tokens{User: "fixture"}, "https://fixture.invalid/", &http.Client{Transport: primaryOwnerRoundTrip(func(r *http.Request) (*http.Response, error) {
					calls++
					require.NoError(t, r.ParseForm())
					require.Equal(t, "/conversations."+method, r.URL.Path)
					require.Equal(t, "second", r.Form.Get("cursor"))
					if calls == 1 {
						return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"2"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true,"messages":[]}`)), Request: r}, nil
				})})
				client.sleep = func(ctx context.Context, delay time.Duration) error {
					sleeps++
					require.Equal(t, 2*time.Second, delay)
					if canceled {
						cancel()
					}
					return ctx.Err()
				}
				var page any
				var err error
				if method == "history" {
					page, err = client.getConversationHistory(ctx, "fixture", &slack.GetConversationHistoryParameters{ChannelID: "C123", Cursor: "second"})
				} else {
					page, err = client.getConversationReplies(ctx, &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000000.000000", Cursor: "second"})
				}
				if canceled {
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, page)
					require.Equal(t, 1, calls)
				} else {
					require.NoError(t, err)
					require.NotNil(t, page)
					require.Equal(t, 2, calls)
				}
				require.Equal(t, 1, sleeps)
			})
		}
	}
}

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
				require.Empty(t, page.Messages)
				require.True(t, page.HasMore)
				require.True(t, page.IsLimited)
				require.Empty(t, page.NextCursor)
			} else {
				page, err := client.getConversationReplies(context.Background(), &slack.GetConversationRepliesParameters{ChannelID: "C123", Timestamp: "1710000001.000000", Limit: 1})
				require.NoError(t, err)
				require.Empty(t, page.Messages)
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

func TestHistoryPageSuccessRetriesPendingInterval(t *testing.T) {
	for _, tc := range []struct {
		name, source, token string
		tokens              config.Tokens
		repair              bool
	}{
		{"bot", SourceBot, "fixture-bot", config.Tokens{Bot: "fixture-bot"}, false},
		{"user", SourceUser, "fixture-user", config.Tokens{User: "fixture-user"}, false},
		{"repair", SourceBot, "fixture-bot", config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			now := time.Unix(1710000200, 0).UTC()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", UpdatedAt: now}))
			require.NoError(t, saveHistoryCoverage(ctx, st, tc.source, "T123", "C123", "", historyCoverage{Complete: true, Latest: "1709900000.000000"}))
			require.NoError(t, st.SetSyncState(ctx, tc.source, "workspace", "T123", "2020-01-01T00:00:00Z"))
			require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "stored-status"))
			const progressQuery = "select * from sync_state where entity_type='workspace' or source_name='doctor' order by source_name,entity_id"
			beforeProgress := repairKeyRows(t, st, progressQuery)
			corrected := false
			var cursors, oldest []string
			var logs bytes.Buffer
			client := primaryOwnerClient(t, tc.tokens, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path != "/conversations.history" {
					return primaryOwnerResponse(r.URL.Path), nil
				}
				require.Equal(t, tc.token, form.Get("token"))
				require.Equal(t, "C123", form.Get("channel"))
				require.Equal(t, "1710000200.000000", form.Get("latest"))
				cursors, oldest = append(cursors, form.Get("cursor")), append(oldest, form.Get("oldest"))
				if form.Get("cursor") == "" {
					return map[string]any{"ok": true, "messages": []any{repairKeyMessage("earlier-page", "1710000000.000000")}, "response_metadata": map[string]any{"next_cursor": "second"}}, nil
				}
				require.Equal(t, "second", form.Get("cursor"))
				if !corrected {
					return map[string]any{"messages": []any{repairKeyMessage("rejected-page-canary", "1710000002.000000")}}, nil
				}
				return map[string]any{"ok": true, "messages": []any{repairKeyMessage("recovered-page", "1710000002.000000")}}, nil
			}).WithLogger(testProgressLogger(&logs))
			client.now = func() time.Time { return now }
			run := func() error {
				if tc.repair {
					return client.repairWorkspace(ctx, st, "T123")
				}
				return client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"})
			}
			runErr := run()
			require.EqualError(t, runErr, "channel C123 history: conversations.history response did not report success")
			require.Equal(t, []string{"", "second"}, cursors)
			require.Equal(t, beforeProgress, repairKeyRows(t, st, progressQuery))
			coverage, err := loadHistoryCoverage(ctx, st, tc.source, "T123", "C123", "")
			require.NoError(t, err)
			require.Equal(t, historyCoverage{Complete: true, Latest: "1709900000.000000", Pending: new("1709896400.000000")}, coverage)
			require.Equal(t, []map[string]any{{"ts": "1710000000.000000", "source_name": tc.source}}, repairKeyRows(t, st, "select ts,source_name from messages order by ts"))
			require.Equal(t, []string{"1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE"}, repairKeyDerived(t, st))
			assertAdmissionCanariesAbsent(t, st, logs.String()+runErr.Error(), "rejected-page-canary", "UREJECTEDPAGECANARY", "FREJECTEDPAGECANARY")
			corrected = true
			require.NoError(t, run())
			require.Equal(t, []string{"", "second", "", "second"}, cursors)
			require.Equal(t, []string{"1709896400.000000", "1709896400.000000", "1709896400.000000", "1709896400.000000"}, oldest)
			coverage, err = loadHistoryCoverage(ctx, st, tc.source, "T123", "C123", "")
			require.NoError(t, err)
			require.Equal(t, historyCoverage{Complete: true, Latest: "1710000200.000000"}, coverage)
			require.Equal(t, []map[string]any{{"ts": "1710000000.000000", "source_name": tc.source}, {"ts": "1710000002.000000", "source_name": tc.source}}, repairKeyRows(t, st, "select ts,source_name from messages order by ts"))
			require.Equal(t, []string{"1710000000.000000|FEARLIERPAGE|earlier-page.txt|UEARLIERPAGE", "1710000002.000000|FRECOVEREDPAGE|recovered-page.txt|URECOVEREDPAGE"}, repairKeyDerived(t, st))
			assertAdmissionCanariesAbsent(t, st, logs.String(), "rejected-page-canary", "UREJECTEDPAGECANARY", "FREJECTEDPAGECANARY")
			if tc.repair {
				require.Equal(t, beforeProgress, repairKeyRows(t, st, progressQuery))
			} else {
				completed, err := st.GetSyncState(ctx, tc.source, "workspace", "T123")
				require.NoError(t, err)
				require.Equal(t, now.Format(time.RFC3339), completed)
			}
		})
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
