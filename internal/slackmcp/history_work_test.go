package slackmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMCPHistoryValidatesRawTimestampsBeforeFiltering(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		for _, ts := range []string{"NaN", "Inf", "-Inf", "1e999", "malformed"} {
			t.Run(string(provider)+"/"+ts, func(t *testing.T) {
				calls := 0
				client := &Client{pageSize: 100, maxPages: 2, mcp: threadWorkSession(func(_ context.Context, _ string, args map[string]any) (string, error) {
					calls++
					payload := map[string]any{"messages": "Channel: one (CONE)\n\n=== Message from User (UONE) at 2024-03-09T16:00:00Z === \nMessage TS: " + ts + "\n" + admissionCanary}
					expected := map[string]any{"channel_id": "CONE", "cursor": "", "oldest": "1710000000.000000", "limit": 100, "response_format": "detailed"}
					if provider == providerReference {
						payload = map[string]any{"ok": true, "messages": []map[string]any{{"ts": ts, "text": admissionCanary}}}
						expected = map[string]any{"channel_id": "CONE", "limit": 100}
					}
					require.Equal(t, expected, args)
					raw, err := json.Marshal(payload)
					return string(raw), err
				})}
				_, err := client.channelMessages(context.Background(), toolset{provider: provider, readChannel: "history"}, "TLOCAL", "CONE", "1710000000.000000", nil)
				require.EqualError(t, err, "MCP message timestamp is invalid")
				require.NotContains(t, err.Error(), admissionCanary)
				require.Equal(t, 1, calls)
			})
		}
	}
}

func TestMCPHistoryWatermarkUsesRawHistory(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		t.Run(string(provider), func(t *testing.T) {
			calls := 0
			client := &Client{pageSize: 100, maxPages: 2, mcp: threadWorkSession(func(_ context.Context, _ string, _ map[string]any) (string, error) {
				calls++
				if provider == providerReference {
					return `{"ok":true,"messages":[{"ts":"9.000000"},{"ts":"10.000001"},{"ts":"1.1e1"}]}`, nil
				}
				ts, next := "9.000000", "cursor `next`"
				if calls == 2 {
					ts, next = "1.1e1", ""
				}
				raw, err := json.Marshal(map[string]any{"messages": "Channel: one (CONE)\n\n=== Message from User (UONE) at 2024-03-09T16:00:00Z === \nMessage TS: " + ts + "\nroot", "pagination_info": next})
				return string(raw), err
			})}
			page, err := client.channelMessages(context.Background(), toolset{provider: provider, readChannel: "history"}, "TLOCAL", "CONE", "100", nil)
			require.NoError(t, err)
			require.Equal(t, "1.1e1", page.LatestTS, "numeric maximum retains its raw key across pages")
			if provider == providerReference {
				require.Empty(t, page.Messages, "all valid raw messages were below the local cutoff")
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
	t.Run("priority", func(t *testing.T) {
		// A richer row can reject materialization without replacing history evidence.
		st, before := coverageStore(t)
		now := time.Unix(1710000000, 0).UTC()
		require.NoError(t, st.EnsureChannel(context.Background(), store.Channel{ID: "CFIRST", WorkspaceID: "TLOCAL", Name: "first", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
		require.NoError(t, st.UpsertMessage(context.Background(), store.Message{WorkspaceID: "TLOCAL", ChannelID: "CFIRST", TS: "1710000000.000001", Text: "richer", NormalizedText: "richer", SourceName: "api-user", SourceRank: 1, RawJSON: "{}", UpdatedAt: now}, nil))
		gateway := newCoverageGateway(t, false, func(_ coverageCall, payload map[string]any) int {
			payload["messages"] = []map[string]any{{"ts": "1710000000.000001", "text": "losing MCP row"}}
			return http.StatusOK
		})
		opts := coverageOptions(t, gateway, admission.Default)
		opts.Channels = []string{"CFIRST"}
		_, err := Sync(context.Background(), st, opts)
		require.NoError(t, err)
		rows, err := st.QueryReadOnly(context.Background(), "select text,source_name from messages")
		require.NoError(t, err)
		require.Equal(t, []map[string]any{{"text": "richer", "source_name": "api-user"}}, rows)
		state := mcpHistoryStateForTest(t, st, "CFIRST", "reference", "")
		require.True(t, state.Complete)
		require.Equal(t, "1710000000.000001", state.Latest)
		require.Nil(t, state.Pending)
		require.NotEqual(t, before, coverageFreshness(t, st))
	})
}

func TestMCPHistorySupersededCompletionStopsSuccess(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		for _, completed := range []bool{false, true} {
			for _, seeded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/complete=%t/existing=%t", provider, completed, seeded), func(t *testing.T) {
					ctx := context.Background()
					path := filepath.Join(t.TempDir(), "history.db")
					st, err := store.Open(path)
					require.NoError(t, err)
					defer func() { require.NoError(t, st.Close()) }()
					other, err := store.Open(path)
					require.NoError(t, err)
					defer func() { require.NoError(t, other.Close()) }()
					now := time.Unix(1710000000, 0).UTC()
					parent := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: "1710000001.000000", Text: "original", NormalizedText: "original", SourceName: SourceName, SourceRank: SourceRank, ReplyCount: 1, RawJSON: "{}", UpdatedAt: now}
					if seeded {
						require.NoError(t, st.EnsureWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
						require.NoError(t, st.EnsureChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "retained", RawJSON: "{}", UpdatedAt: now}))
						require.NoError(t, st.UpsertMessage(ctx, parent, nil))
						_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123", TS: parent.TS}}})
						require.NoError(t, err)
					}
					type renewal struct {
						work store.MCPHistoryWork
						rows map[string][]map[string]any
						err  error
					}
					renewed := make(chan renewal, 1)
					calls := make(chan string, 4)
					gateway := admissionGateway(t, provider == providerReference, func(tool string, _ map[string]any) map[string]any {
						calls <- tool
						historyTool := "slack_read_channel"
						if provider == providerReference {
							historyTool = "slack_get_channel_history"
						}
						if tool != historyTool {
							return map[string]any{"error": "unexpected-tool"}
						}
						work, selected, err := other.BeginMCPHistory(ctx, store.MCPHistoryScope{WorkspaceID: "TLOCAL", ChannelID: "C123", Adapter: string(provider)}, store.MCPHistoryOptions{})
						if err == nil && !selected {
							err = fmt.Errorf("new history was not selected")
						}
						if err == nil && seeded {
							newer := parent
							newer.Text, newer.NormalizedText, newer.RawJSON = "newer winner", "newer winner", `{"text":"newer winner"}`
							_, err = other.ApplyWriteBatch(ctx, store.WriteBatch{MCPHistoryGuard: &work, Messages: []store.MessageWrite{{Message: newer}}})
							if err == nil {
								_, err = other.PrepareMCPThreadWork(ctx, work)
							}
						}
						if err == nil && completed {
							var done bool
							done, err = other.CompleteMCPHistory(ctx, work, "")
							if err == nil && !done {
								err = fmt.Errorf("new history did not complete")
							}
						}
						renewed <- renewal{work: work, rows: admissionTableSnapshot(t, other), err: err}
						if provider == providerReference {
							return map[string]any{"ok": true, "messages": []map[string]any{{"ts": parent.TS, "text": admissionCanary + " <@USTALE>", "reply_count": 1}}}
						}
						return map[string]any{"messages": "Channel: " + admissionCanary + " (C123)\n\n=== Message from Fixture (UONE) at 2024-03-09T16:00:00Z === \nMessage TS: " + parent.TS + "\n" + admissionCanary + " <@USTALE>\nThread: 1 replies (latest: 2024-03-09T16:00:01Z)"}
					})
					defer gateway.Close()
					t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
					cfg := testMCPConfig(gateway.URL)
					cfg.ConnectorID = ""
					summary, err := Sync(ctx, st, Options{Config: cfg, WorkspaceID: "TLOCAL", Channels: []string{"C123"}})
					require.ErrorIs(t, err, store.ErrMCPHistorySuperseded)
					require.NotContains(t, err.Error(), admissionCanary)
					owner := <-renewed
					require.NoError(t, owner.err)
					require.Zero(t, summary.Channels)
					require.Zero(t, summary.Messages)
					require.Zero(t, summary.Replies)
					require.Len(t, calls, 1)
					require.Equal(t, owner.rows, admissionTableSnapshot(t, st), "stale channel metadata, equal-rank rows, derived data and job renewal all stay absent")
					state := mcpHistoryStateForTest(t, st, "C123", string(provider), "")
					require.Equal(t, owner.work.Revision, state.Revision)
					require.Equal(t, completed, state.Complete)
					if completed {
						require.Nil(t, state.Pending)
					} else {
						require.Equal(t, new(""), state.Pending)
					}
				})
			}
		}
	}
}

func mcpHistoryStateForTest(t *testing.T, st *store.Store, channel, adapter, since string) store.MCPHistoryState {
	t.Helper()
	key, err := json.Marshal([]string{"TLOCAL", channel, adapter, since})
	require.NoError(t, err)
	value, err := st.GetSyncState(context.Background(), SourceName, store.MCPHistoryEntityType, string(key))
	require.NoError(t, err)
	var state store.MCPHistoryState
	require.NoError(t, json.Unmarshal([]byte(value), &state))
	require.NotEmpty(t, state.Revision, fmt.Sprint(state))
	return state
}
