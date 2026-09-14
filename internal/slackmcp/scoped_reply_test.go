package slackmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

// The competing owner runs inside the tools/call callback. This composes the
// real acquisition/write owners without claiming two concurrent whole Syncs.
func TestMCPScopedRepliesRejectSupersededResponses(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		for _, completed := range []bool{false, true} {
			for _, mode := range []string{"data", "empty", "error", "malformed", "incomplete", "second-page", "deleted"} {
				if mode == "deleted" && completed {
					continue
				}
				if mode == "second-page" && provider != providerCodex {
					continue
				}
				t.Run(fmt.Sprintf("%s/complete=%t/%s", provider, completed, mode), func(t *testing.T) {
					ctx := context.Background()
					st, other, history, first := scopedReplyStores(t)
					now := time.Unix(1710000000, 0).UTC()
					calls := 0
					var afterOwner map[string][]map[string]any
					client := &Client{pageSize: 100, maxPages: 2, mcp: threadWorkSession(func(_ context.Context, _ string, args map[string]any) (string, error) {
						calls++
						expected := map[string]any{"channel_id": "C123", "message_ts": first.TS, "cursor": "", "limit": 100, "response_format": "detailed"}
						if provider == providerReference {
							expected = map[string]any{"channel_id": "C123", "thread_ts": first.TS}
						}
						if calls == 2 {
							expected["cursor"] = "private-cursor-canary"
						}
						require.Equal(t, expected, args, "Since selects the root, not a date-bounded reply request")
						if mode == "second-page" && calls == 1 {
							return scopedReplyPayload(t, provider, first.TS, "incomplete"), nil
						}
						scope := history.MCPHistoryScope
						scope.Since, scope.Adapter = "200", "codex"
						next, selected, err := other.BeginMCPHistory(ctx, scope, store.MCPHistoryOptions{})
						require.NoError(t, err)
						require.True(t, selected)
						second, err := other.PrepareMCPReturnedThreadWork(ctx, next, []string{first.TS}, nil)
						require.NoError(t, err)
						require.Len(t, second, 1)
						require.NotEqual(t, first.Generation, second[0].Generation)
						parent := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: first.TS, Text: "newer winner", NormalizedText: "newer winner", ReplyCount: 1, SourceName: SourceName, SourceRank: SourceRank, RawJSON: "{}", UpdatedAt: now}
						child := parent
						child.TS, child.ThreadTS, child.ReplyCount = "1710000002.000000", first.TS, 0
						result, err := other.ApplyWriteBatch(ctx, store.WriteBatch{ThreadGuard: &second[0], Messages: []store.MessageWrite{{Message: parent}, {Message: child}}})
						require.NoError(t, err)
						require.Equal(t, 2, result.MessagesWritten)
						if mode == "deleted" {
							parent.DeletedTS = "1710000009.000000"
							require.NoError(t, other.MarkMessageDeleted(ctx, parent, nil))
						}
						if completed {
							done, err := other.CompleteThreadWork(ctx, second[0], "", nil)
							require.NoError(t, err)
							require.True(t, done)
						}
						afterOwner = admissionTableSnapshot(t, other)
						if mode == "error" {
							return "", errors.New(admissionCanary)
						}
						if mode == "malformed" {
							return "{" + admissionCanary, nil
						}
						return scopedReplyPayload(t, provider, first.TS, mode), nil
					})}
					result, err := syncThread(ctx, st, client, toolset{provider: provider, readThread: "replies"}, first, false, now)
					require.NoError(t, err, "supersession discards the stale response error too")
					require.Equal(t, threadSyncResult{revoked: true}, result)
					wantCalls := 1
					if mode == "second-page" {
						wantCalls = 2
					}
					require.Equal(t, wantCalls, calls, "no subsequent page or response-driven reacquisition")
					require.Equal(t, afterOwner, admissionTableSnapshot(t, st), "newer same-rank parent/child and every derived row/job remain exact")
					done, err := st.CompleteThreadWork(ctx, first, "", nil)
					require.NoError(t, err)
					require.False(t, done)
					require.Equal(t, afterOwner, admissionTableSnapshot(t, st))
				})
			}
		}
	}
}

func TestMCPScopedReplyOutcomesKeepOwnedLifecycle(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		for _, mode := range []string{"data", "empty", "error", "malformed", "identity", "canceled"} {
			t.Run(string(provider)+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				st, _, _, work := scopedReplyStores(t)
				before := admissionTableSnapshot(t, st)
				calls := 0
				requestErr := errors.New("synthetic request failure")
				client := &Client{pageSize: 100, maxPages: 2, mcp: threadWorkSession(func(context.Context, string, map[string]any) (string, error) {
					calls++
					switch mode {
					case "error":
						return "", requestErr
					case "malformed":
						return "{" + admissionCanary, nil
					case "canceled":
						cancel()
					}
					return scopedReplyPayload(t, provider, work.TS, mode), nil
				})}
				result, err := syncThread(ctx, st, client, toolset{provider: provider, readThread: "replies"}, work, false, time.Unix(1710000000, 0).UTC())
				require.Equal(t, 1, calls)
				switch mode {
				case "data", "empty":
					require.NoError(t, err)
					require.False(t, result.revoked)
					if mode == "data" {
						require.Equal(t, 1, result.replies)
					} else {
						require.Zero(t, result.replies)
						require.Equal(t, before, admissionTableSnapshot(t, st), "empty traversal still passes the write guard")
					}
					done, err := st.CompleteThreadWork(context.Background(), work, "", nil)
					require.NoError(t, err)
					require.True(t, done)
					pending, err := st.PendingThreadWork(context.Background(), SourceName, work.WorkspaceID, work.ChannelID)
					require.NoError(t, err)
					require.Empty(t, pending)
				default:
					require.Error(t, err)
					if mode == "error" {
						require.ErrorIs(t, err, requestErr)
					} else if mode == "canceled" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NotContains(t, err.Error(), admissionCanary)
					}
					require.Equal(t, before, admissionTableSnapshot(t, st), "invalid or canceled replies keep the exact selected generation")
				}
			})
		}
	}
}

func scopedReplyStores(t *testing.T) (*store.Store, *store.Store, store.MCPHistoryWork, store.ThreadWork) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scoped-replies.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	other, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	root := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: "1710000001.000000", Text: "original", NormalizedText: "original", ReplyCount: 1, SourceName: SourceName, SourceRank: SourceRank, RawJSON: "{}", UpdatedAt: now}
	require.NoError(t, st.UpsertMessage(ctx, root, nil))
	history, selected, err := st.BeginMCPHistory(ctx, store.MCPHistoryScope{WorkspaceID: "TLOCAL", ChannelID: "C123", Adapter: "reference", Since: "100"}, store.MCPHistoryOptions{})
	require.NoError(t, err)
	require.True(t, selected)
	done, err := st.CompleteMCPHistory(ctx, history, root.TS)
	require.NoError(t, err)
	require.True(t, done)
	work, err := st.PrepareMCPReturnedThreadWork(ctx, history, []string{root.TS}, nil)
	require.NoError(t, err)
	require.Len(t, work, 1)
	return st, other, history, work[0]
}

func scopedReplyPayload(t *testing.T, provider providerKind, root, mode string) string {
	t.Helper()
	if mode == "identity" {
		root = "1710000999.000000"
	}
	var payload map[string]any
	if provider == providerReference {
		messages := []map[string]any{}
		if mode != "empty" {
			messages = append(messages, map[string]any{"ts": root, "text": admissionCanary}, map[string]any{"ts": "1710000002.000000", "thread_ts": root, "text": admissionCanary + " <@USTALE>"})
		}
		payload = map[string]any{"ok": true, "messages": messages}
		if mode == "incomplete" {
			payload["has_more"], payload["is_limited"] = true, true
		}
	} else {
		messages := "From: Fixture (UONE)\nTime: 2024-03-09T16:00:00Z\nMessage TS: " + root + "\n" + admissionCanary
		if mode != "empty" {
			messages += "\n\n=== THREAD REPLIES\n\n--- Reply 1 ---\nFrom: Fixture (UONE)\nTime: 2024-03-09T16:00:01Z\nMessage TS: 1710000002.000000\n" + admissionCanary + " <@USTALE>"
		}
		payload = map[string]any{"messages": messages, "pagination_info": ""}
		if mode == "incomplete" {
			payload["pagination_info"] = "next `private-cursor-canary`"
		}
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return string(raw)
}
