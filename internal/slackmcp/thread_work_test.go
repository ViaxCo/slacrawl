package slackmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/mcpclient"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestMCPThreadWorkRevocation(t *testing.T) {
	for _, mode := range []string{"before-request", "delete-in-flight", "renew-in-flight", "error-revoked", "invalid-revoked", "empty-revoked", "empty-current", "error-current", "parent-commit-renews", "unguarded"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := admissionStore(t)
			now := time.Unix(1710000000, 0).UTC()
			parent := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: "1710000001.000000", Text: "root", NormalizedText: "root", SourceName: SourceName, SourceRank: SourceRank, RawJSON: "{}", UpdatedAt: now}
			require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.UpsertMessage(ctx, parent, nil))
			queued, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123", TS: parent.TS}}})
			require.NoError(t, err)
			require.Len(t, queued.PendingThreads, 1)
			work := queued.PendingThreads[0]
			var expected map[string][]map[string]any
			deleteParent := func() {
				deleted := parent
				deleted.DeletedTS, deleted.SourceName, deleted.SourceRank = "1710000009.000000", "api-user", 1
				require.NoError(t, st.MarkMessageDeleted(ctx, deleted, nil))
				expected = admissionTableSnapshot(t, st)
			}
			renew := func() {
				_, err := st.DB().ExecContext(ctx, "update sync_state set value='new-generation' where source_name='mcp' and entity_type='thread_pending_v1'")
				require.NoError(t, err)
				expected = admissionTableSnapshot(t, st)
			}
			guard := &work
			if mode == "before-request" {
				deleteParent()
			} else if mode == "unguarded" {
				renew()
				guard = nil
			}
			if mode == "parent-commit-renews" {
				// Force renewal at the real boundary between the existing separate
				// parent and reply commits, without adding a production test hook.
				_, err := st.DB().ExecContext(ctx, `create trigger renew_after_parent_update after update on messages
when new.channel_id='C123' and new.ts='1710000001.000000'
begin update sync_state set value='new-generation' where source_name='mcp' and entity_type='thread_pending_v1'; end`)
				require.NoError(t, err)
			}
			calls := 0
			client := &Client{mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
				calls++
				require.Equal(t, "slack_get_thread_replies", tool)
				require.Equal(t, map[string]any{"channel_id": "C123", "thread_ts": parent.TS}, args)
				switch mode {
				case "delete-in-flight", "error-revoked", "invalid-revoked":
					deleteParent()
				case "renew-in-flight", "empty-revoked":
					renew()
				}
				if mode == "error-current" || mode == "error-revoked" {
					return "", errors.New("synthetic request failure")
				}
				if mode == "invalid-revoked" {
					return "not JSON", nil
				}
				if mode == "empty-current" || mode == "empty-revoked" {
					return `{"ok":true,"messages":[]}`, nil
				}
				return `{"ok":true,"messages":[{"ts":"1710000001.000000","text":"root refreshed"},{"ts":"1710000002.000000","thread_ts":"1710000001.000000","text":"child <@UCHILD>"}]}`, nil
			})}
			before := admissionTableSnapshot(t, st)
			result, err := syncThread(ctx, st, client, toolset{provider: providerReference, readThread: "slack_get_thread_replies"}, "TLOCAL", "C123", parent.TS, false, now, guard)
			if mode == "error-current" {
				require.EqualError(t, err, "read MCP thread: synthetic request failure")
				require.Equal(t, before, admissionTableSnapshot(t, st))
				return
			}
			require.NoError(t, err)
			require.Equal(t, map[bool]int{true: 0, false: 1}[mode == "before-request"], calls)
			if mode == "unguarded" {
				require.False(t, result.revoked)
				require.Equal(t, 1, result.replies)
			} else if mode == "empty-current" {
				require.False(t, result.revoked)
				require.Zero(t, result.replies)
				require.Equal(t, before, admissionTableSnapshot(t, st))
			} else {
				require.True(t, result.revoked)
				require.Zero(t, result.replies)
				require.Equal(t, messageCoverage{}, result.coverage)
				if mode == "parent-commit-renews" {
					rows, err := st.QueryReadOnly(ctx, "select ts,text,reply_count from messages order by ts")
					require.NoError(t, err)
					require.Equal(t, []map[string]any{{"ts": parent.TS, "text": "root refreshed", "reply_count": int64(1)}}, rows, "the already committed parent remains without the stale reply")
					pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
					require.NoError(t, err)
					require.Len(t, pending, 1)
					require.Equal(t, "new-generation", pending[0].Generation)
				} else {
					require.Equal(t, expected, admissionTableSnapshot(t, st), "stale data must not reach any message or derived-state write")
				}
			}
		})
	}
}

func TestMCPThreadWorkChecksBetweenTextPages(t *testing.T) {
	for _, mode := range []string{"renew-second-page", "callback-error", "current"} {
		t.Run(mode, func(t *testing.T) {
			current, calls := true, 0
			client := &Client{pageSize: 100, mcp: threadWorkSession(func(_ context.Context, _ string, args map[string]any) (string, error) {
				calls++
				cursor := "next"
				if calls == 2 {
					require.Equal(t, "next", args["cursor"])
					cursor = ""
					if mode != "current" {
						current = false
					}
				}
				raw, err := json.Marshal(map[string]any{"messages": "From: Fixture (UONE)\nTime: 2024-03-09T16:00:00Z\nMessage TS: 1710000001.000000\nparent\n\n=== THREAD REPLIES\n\n--- Reply 1 ---\nFrom: Fixture (UONE)\nTime: 2024-03-09T16:00:01Z\nMessage TS: 1710000002.000000\nchild", "pagination_info": map[bool]string{true: "next `next`", false: ""}[cursor != ""]})
				return string(raw), err
			})}
			result, err := client.threadMessages(context.Background(), toolset{readThread: "slack_read_thread"}, "TLOCAL", "C123", "1710000001.000000", func() (bool, error) {
				if mode == "callback-error" && !current {
					return false, context.Canceled
				}
				return current, nil
			})
			require.Equal(t, 2, calls)
			if mode == "callback-error" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				if mode == "current" {
					require.False(t, result.revoked)
					require.Len(t, result.Replies, 2)
				} else {
					require.Equal(t, threadPage{revoked: true}, result, "revocation must discard earlier materialized pages too")
				}
			}
		})
	}
}

func TestMCPThreadWorkAcrossMessageBatches(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	var mu sync.Mutex
	var calls []string
	retry := false
	message := func(i, hints int) map[string]any {
		return map[string]any{"ts": fmt.Sprintf("171000%04d.000000", i), "text": fmt.Sprintf("message %d", i), "reply_count": hints}
	}
	gateway := admissionGateway(t, true, func(tool string, args map[string]any) map[string]any {
		mu.Lock()
		isRetry := retry
		mu.Unlock()
		payload := map[string]any{"ok": true}
		switch tool {
		case "slack_get_channel_history":
			messages := []map[string]any{}
			if !isRetry {
				for i := 1; i <= 65; i++ {
					messages = append(messages, message(i, 1))
				}
				for i := 66; i <= 435; i++ {
					messages = append(messages, message(i, 0))
				}
				for i := 1; i <= 65; i++ {
					messages = append(messages, message(i, 0))
				}
				messages = append(messages, message(1, 0)) // Row 501 crosses the transaction boundary.
			} else {
				messages = append(messages, message(1, 0))
			}
			payload["messages"] = messages
		case "slack_get_thread_replies":
			mu.Lock()
			calls = append(calls, args["thread_ts"].(string))
			mu.Unlock()
			if !isRetry {
				payload["error"] = "synthetic_failure"
			} else {
				payload["messages"] = []map[string]any{}
			}
		default:
			payload["error"] = "unexpected_tool"
		}
		return payload
	})
	defer gateway.Close()
	t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
	t.Setenv("TEST_MCP_ACCOUNT", "")
	cfg := testMCPConfig(gateway.URL)
	cfg.ConnectorID, cfg.AuthPath = "", filepath.Join(t.TempDir(), "auth.json")
	opts := Options{Config: cfg, WorkspaceID: "TLOCAL", Channels: []string{"C123"}, Full: true}
	summary, err := Sync(ctx, st, opts)
	require.EqualError(t, err, "read MCP thread: decode reference Slack thread: Slack API reported an error")
	require.Equal(t, 501, summary.Messages)
	pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
	require.NoError(t, err)
	require.Len(t, pending, 65, "the authoritative queue must not inherit a reporting limit")
	rows, err := st.QueryReadOnly(ctx, "select ts from messages where reply_count>0")
	require.NoError(t, err)
	require.Empty(t, rows, "every hint was overwritten before the failed thread request")
	before := make([]string, 0, len(pending))
	for _, work := range pending {
		before = append(before, work.TS)
	}
	require.NoError(t, st.Close())
	st, err = store.Open(path)
	require.NoError(t, err)
	mu.Lock()
	retry, calls = true, nil
	mu.Unlock()
	_, err = Sync(ctx, st, opts)
	require.NoError(t, err)
	mu.Lock()
	observed := append([]string(nil), calls...)
	mu.Unlock()
	require.Equal(t, before, observed, "restart drains all generations once, in timestamp order")
	pending, err = st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestMCPStoredTombstoneBeforeHistoryWrites(t *testing.T) {
	for _, marker := range []string{"deleted_ts", "subtype"} {
		t.Run(marker, func(t *testing.T) {
			ctx := context.Background()
			st, _ := coverageStore(t)
			now := time.Unix(1, 0).UTC()
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "CFIRST", WorkspaceID: "TLOCAL", Name: "first", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			parent := store.Message{WorkspaceID: "TLOCAL", ChannelID: "CFIRST", TS: "1710000000.000001", Text: "alpha", ReplyCount: 1, SourceName: "api-user", SourceRank: 1, RawJSON: "{}", UpdatedAt: now}
			require.NoError(t, st.UpsertMessage(ctx, parent, nil))
			_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "CFIRST", TS: parent.TS}}})
			require.NoError(t, err)
			gateway := newCoverageGateway(t, true, func(call coverageCall, payload map[string]any) int {
				if call == (coverageCall{"CFIRST", ""}) {
					deleted := parent
					if marker == "deleted_ts" {
						deleted.DeletedTS = "1710000009.000000"
					} else {
						deleted.Subtype = "message_deleted"
					}
					err := st.UpsertMessage(ctx, deleted, nil)
					if err != nil {
						return http.StatusInternalServerError
					}
					// The other live root remains ordinary work. No hidden deletion
					// event is supplied through the history response.
					payload["messages"] = payload["messages"].([]map[string]any)[1:]
				}
				return http.StatusOK
			})
			_, err = Sync(ctx, st, coverageOptions(t, gateway, admission.Exclude))
			require.NoError(t, err)
			require.Equal(t, []coverageCall{{"CFIRST", ""}, {"CFIRST", "1710000010.000003"}, {"CSECOND", ""}}, gateway.dataCalls(t))
			pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "CFIRST")
			require.NoError(t, err)
			require.Empty(t, pending)
			stored, err := st.QueryReadOnly(ctx, "select coalesce(deleted_ts,'') as deleted_ts,coalesce(subtype,'') as subtype from messages where channel_id='CFIRST' and ts='1710000000.000001'")
			require.NoError(t, err)
			require.Len(t, stored, 1)
			require.True(t, stored[0]["deleted_ts"] != "" || stored[0]["subtype"] == "message_deleted")
		})
	}
}

func TestMCPRevokedNativeCoverageDoesNotFailSync(t *testing.T) {
	ctx := context.Background()
	st, before := coverageStore(t)
	// Only the thread parent update renews work; history insertion and its
	// generation remain current until decoding and that first commit finish.
	_, err := st.DB().ExecContext(ctx, `create trigger renew_flagged_parent after update on messages
when new.channel_id='CFIRST' and new.ts='1710000000.000001' and new.text='renewed parent'
begin update sync_state set value='new-generation' where source_name='mcp' and entity_type='thread_pending_v1' and entity_id='["TLOCAL","CFIRST","1710000000.000001"]'; end`)
	require.NoError(t, err)
	gateway := newCoverageGateway(t, true, func(call coverageCall, payload map[string]any) int {
		if call == (coverageCall{"CFIRST", "1710000000.000001"}) {
			payload["messages"].([]map[string]any)[0]["text"] = "renewed parent"
			payload["has_more"], payload["is_limited"] = true, true
		}
		return http.StatusOK
	})
	_, err = Sync(ctx, st, coverageOptions(t, gateway, admission.Exclude))
	require.NoError(t, err, "revoked response coverage must not fail unrelated completed work")
	require.NotEqual(t, before, coverageFreshness(t, st))
	require.Equal(t, []coverageCall{{"CFIRST", ""}, {"CFIRST", "1710000000.000001"}, {"CFIRST", "1710000010.000003"}, {"CSECOND", ""}}, gateway.dataCalls(t))
	pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "CFIRST")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "1710000000.000001", pending[0].TS)
	require.Equal(t, "new-generation", pending[0].Generation)
	rows, err := st.QueryReadOnly(ctx, "select ts,text from messages where channel_id='CFIRST' and ts in ('1710000000.000001','1710000001.000002') order by ts")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"ts": "1710000000.000001", "text": "renewed parent"}}, rows, "retain the committed parent, discard its stale child")
}

// Compose the real materialize/Prepare/write boundaries with two Store handles.
// A history RPC callback is before Prepare in MCP and cannot model this race.
func TestMCPAdmittedRevivalRequeuesCanceledWork(t *testing.T) {
	for _, mode := range []string{"hint", "duplicate-hint", "child", "renewed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "archive.db")
			st, err := store.Open(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			other, err := store.Open(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, other.Close()) }()
			now := time.Unix(1710000000, 0).UTC()
			parent := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: "1710000001.000000", Text: "root", NormalizedText: "root", ReplyCount: 1, SourceName: SourceName, SourceRank: SourceRank, RawJSON: "{}", UpdatedAt: now}
			require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, st.UpsertMessage(ctx, parent, nil))
			require.NoError(t, st.SetSyncState(ctx, SourceName, "workspace", "TLOCAL", "prior-success"))
			prepared, err := st.PrepareThreadWork(ctx, SourceName, "TLOCAL", "C123", nil)
			require.NoError(t, err)
			require.Len(t, prepared, 1)
			materialized := MessageRecord{ChannelID: "C123", TS: parent.TS, Text: "revived root", ReplyCount: 1}
			deleted := parent
			deleted.DeletedTS = "1710000009.000000"
			require.NoError(t, other.MarkMessageDeleted(ctx, deleted, nil))
			pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
			require.NoError(t, err)
			require.Empty(t, pending)
			if mode == "renewed" {
				require.NoError(t, other.UpsertMessage(ctx, parent, nil))
				_, err := other.PrepareThreadWork(ctx, SourceName, "TLOCAL", "C123", nil)
				require.NoError(t, err)
			}
			before, err := st.QueryReadOnly(ctx, "select * from sync_state order by entity_type,entity_id")
			require.NoError(t, err)
			batch := store.WriteBatch{Messages: []store.MessageWrite{toMessageWrite("TLOCAL", materialized, false, now)},
				PendingThreads:  []store.ThreadWork{{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123", TS: parent.TS}},
				ThreadDiscovery: &store.ThreadWorkDiscovery{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123"}}
			if mode == "duplicate-hint" {
				materialized.ReplyCount = 0
				batch.Messages = append(batch.Messages, toMessageWrite("TLOCAL", materialized, false, now))
			} else if mode == "child" {
				materialized.ReplyCount = 0
				batch.Messages[0] = toMessageWrite("TLOCAL", materialized, false, now)
				child := MessageRecord{ChannelID: "C123", TS: "1710000002.000000", ThreadTS: parent.TS, Text: "child"}
				batch.Messages = append(batch.Messages, toMessageWrite("TLOCAL", child, false, now))
				batch.PendingThreads = nil
			}
			written, err := st.ApplyWriteBatch(ctx, batch)
			require.NoError(t, err)
			calls := 0
			client := &Client{mcp: threadWorkSession(func(_ context.Context, _ string, _ map[string]any) (string, error) {
				calls++
				return "{\"ok\":true,\"messages\":[]}", nil
			})}
			tools := toolset{provider: providerReference, readThread: "slack_get_thread_replies"}
			stale, err := syncThread(ctx, st, client, tools, "TLOCAL", "C123", parent.TS, false, now, &prepared[0])
			require.NoError(t, err)
			require.True(t, stale.revoked)
			require.Zero(t, calls)
			if mode == "renewed" {
				require.Empty(t, written.PendingThreads)
				after, err := st.QueryReadOnly(ctx, "select * from sync_state order by entity_type,entity_id")
				require.NoError(t, err)
				require.Equal(t, before, after, "do not adopt or renew another writer's generation")
			} else {
				require.Len(t, written.PendingThreads, 1)
				work := written.PendingThreads[0]
				require.NotEqual(t, prepared[0].Generation, work.Generation)
				result, err := syncThread(ctx, st, client, tools, "TLOCAL", "C123", parent.TS, false, now, &work)
				require.NoError(t, err)
				require.False(t, result.revoked)
				require.Equal(t, 1, calls)
				completed, err := st.CompleteThreadWork(ctx, work, "", nil)
				require.NoError(t, err)
				require.True(t, completed)
				pending, err = st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
				require.NoError(t, err)
				require.Empty(t, pending)
			}
			freshness, err := st.GetSyncState(ctx, SourceName, "workspace", "TLOCAL")
			require.NoError(t, err)
			require.Equal(t, "prior-success", freshness, "this composed owner proof does not run full Sync")
		})
	}
}

// Compose empty preparation and a competing page owner; this does not run
// full Sync or claim a concurrent CLI/freshness execution.
func TestMCPHistoryPreservesUnseenConcurrentWork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	owner, err := store.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.SetSyncState(ctx, SourceName, "workspace", "TLOCAL", "prior-success"))
	prepared, err := st.PrepareThreadWork(ctx, SourceName, "TLOCAL", "C123", nil)
	require.NoError(t, err)
	require.Empty(t, prepared)
	materialized := MessageRecord{ChannelID: "C123", TS: "1710000001.000000", Text: "root", ReplyCount: 1}
	batch := store.WriteBatch{
		Messages:        []store.MessageWrite{toMessageWrite("TLOCAL", materialized, false, now)},
		PendingThreads:  []store.ThreadWork{{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123", TS: materialized.TS}},
		ThreadDiscovery: &store.ThreadWorkDiscovery{SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "C123"},
	}
	owned, err := owner.ApplyWriteBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, owned.PendingThreads, 1)
	before, err := owner.QueryReadOnly(ctx, "select * from sync_state order by entity_type,entity_id")
	require.NoError(t, err)
	written, err := st.ApplyWriteBatch(ctx, batch)
	require.NoError(t, err)
	require.Empty(t, written.PendingThreads, "the contender must not adopt or renew the unseen job")
	after, err := st.QueryReadOnly(ctx, "select * from sync_state order by entity_type,entity_id")
	require.NoError(t, err)
	require.Equal(t, before, after, "preserve the entire owner job including its timestamp")
	calls := 0
	client := &Client{mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
		calls++
		require.Equal(t, "slack_get_thread_replies", tool)
		require.Equal(t, map[string]any{"channel_id": "C123", "thread_ts": materialized.TS}, args)
		return `{"ok":true,"messages":[]}`, nil
	})}
	work := owned.PendingThreads[0]
	result, err := syncThread(ctx, owner, client, toolset{provider: providerReference, readThread: "slack_get_thread_replies"}, "TLOCAL", "C123", materialized.TS, false, now, &work)
	require.NoError(t, err)
	require.False(t, result.revoked)
	require.Equal(t, 1, calls)
	completed, err := owner.CompleteThreadWork(ctx, work, "", nil)
	require.NoError(t, err)
	require.True(t, completed)
	pending, err := st.PendingThreadWork(ctx, SourceName, "TLOCAL", "C123")
	require.NoError(t, err)
	require.Empty(t, pending)
	freshness, err := st.GetSyncState(ctx, SourceName, "workspace", "TLOCAL")
	require.NoError(t, err)
	require.Equal(t, "prior-success", freshness)
}

type threadWorkSession func(context.Context, string, map[string]any) (string, error)

func (threadWorkSession) Initialize(context.Context) error                    { return nil }
func (threadWorkSession) ListTools(context.Context) ([]mcpclient.Tool, error) { return nil, nil }
func (threadWorkSession) Close() error                                        { return nil }
func (f threadWorkSession) CallToolText(ctx context.Context, name string, args map[string]any) (string, error) {
	return f(ctx, name, args)
}
