package slackapi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/slack-go/slack/slackevents"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/share"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestRetainedThreadScope(t *testing.T) {
	for _, mode := range []string{"ordinary", "full", "since", "full-since", "repair", "excluded-name", "other-channel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			retainedOwnerSeed(t, st, "1710000001.000000")
			_, err := st.PrepareThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			before, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			replies, histories := 0, 0
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				switch r.URL.Path {
				case "/conversations.history":
					histories++
				case "/conversations.replies":
					replies++
					require.Equal(t, "1710000001.000000", form.Get("ts"))
					return map[string]any{"ok": true, "messages": []any{}}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			opts := SyncOptions{WorkspaceID: "T123"}
			switch mode {
			case "full", "full-since":
				opts.Full = true
			case "excluded-name":
				opts.ExcludeChannels = []string{"fixture"}
			case "other-channel":
				opts.Channels = []string{"COTHER"}
			}
			if mode == "since" || mode == "full-since" {
				opts.Since = "1710000000.000000"
			}
			if mode == "repair" {
				err = client.repairWorkspace(ctx, st, "T123")
			} else {
				err = client.Sync(ctx, st, opts)
			}
			require.NoError(t, err)
			after, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
			require.NoError(t, err)
			if mode == "ordinary" || mode == "full" {
				require.Equal(t, 1, replies)
				require.Empty(t, after)
			} else {
				require.Zero(t, replies)
				require.Equal(t, before, after)
			}
			require.Equal(t, map[bool]int{true: 0, false: 1}[mode == "excluded-name" || mode == "other-channel"], histories)
		})
	}
}

func TestRetainedThreadDrainAndNewGeneration(t *testing.T) {
	for _, renew := range []bool{false, true} {
		t.Run(fmt.Sprintf("renew=%t", renew), func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			for i := 0; i < 61; i++ {
				retainedOwnerSeed(t, st, fmt.Sprintf("17100000%02d.000000", i))
			}
			calls := map[string][]string{}
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path == "/conversations.replies" {
					ts, cursor := form.Get("ts"), form.Get("cursor")
					calls[ts] = append(calls[ts], cursor)
					payload := map[string]any{"ok": true, "messages": []any{}}
					if cursor == "" {
						payload["response_metadata"] = map[string]any{"next_cursor": "second"}
					} else {
						require.Equal(t, "second", cursor)
						if renew {
							_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceUser, WorkspaceID: "T123", ChannelID: "C123", TS: ts}}})
							require.NoError(t, err)
						}
					}
					return payload, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
			require.Len(t, calls, 61)
			for _, cursors := range calls {
				require.Equal(t, []string{"", "second"}, cursors)
			}
			pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Len(t, pending, map[bool]int{true: 61, false: 0}[renew])
			status, err := st.Status(ctx)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{true: "partial", false: "full"}[renew], status.ThreadState)
		})
	}
}

func TestRetainedThreadFailuresKeepWork(t *testing.T) {
	for _, mode := range []string{"history-error", "history-incomplete", "replies-error", "replies-incomplete", "canceled", "skip"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			retainedOwnerSeed(t, st, "1710000001.000000")
			prior := historyCoverage{Complete: true, Latest: "1710000200.000000"}
			require.NoError(t, saveHistoryCoverage(ctx, st, SourceBot, "T123", "C123", "", prior))
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
				if r.URL.Path == "/conversations.history" {
					if mode == "history-error" {
						return map[string]any{"ok": false, "error": "synthetic_history_failure"}, nil
					}
					if mode == "history-incomplete" {
						return map[string]any{"ok": true, "messages": []any{}, "has_more": true}, nil
					}
				}
				if r.URL.Path == "/conversations.replies" {
					if mode == "canceled" {
						cancel()
						return nil, ctx.Err()
					}
					if mode == "replies-incomplete" {
						return map[string]any{"ok": true, "messages": []any{}, "has_more": true}, nil
					}
					code := "synthetic_replies_failure"
					if mode == "skip" {
						code = "missing_scope"
					}
					return map[string]any{"ok": false, "error": code}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			err := client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"})
			if mode == "skip" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			pending, err := st.PendingThreadWork(context.Background(), SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Len(t, pending, 1)
			coverage, err := loadHistoryCoverage(context.Background(), st, SourceBot, "T123", "C123", "")
			require.NoError(t, err)
			if mode != "skip" {
				require.Equal(t, prior.Latest, coverage.Latest)
				require.True(t, coverage.Complete)
				require.Equal(t, new("1709996600.000000"), coverage.Pending)
			}
		})
	}
}

func TestTailDeletionRetiresPendingThread(t *testing.T) {
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	ctx := context.Background()
	retainedOwnerSeed(t, st, "1710000001.000000")
	for _, source := range []string{SourceUser, "mcp"} {
		_, err := st.PrepareThreadWork(ctx, source, "T123", "C123")
		require.NoError(t, err)
	}
	require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|C123|1710000001.000000", "missing_scope"))
	client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot"}, func(r *http.Request, _ url.Values) (any, error) {
		t.Fatalf("typed event should not make an API request: %s", r.URL.Path)
		return nil, nil
	})
	client.now = func() time.Time { return time.Unix(1710000100, 0).UTC() }
	event, err := slackevents.ParseEvent([]byte(`{"type":"event_callback","team_id":"T123","event":{"type":"message","subtype":"message_deleted","channel":"C123","channel_type":"channel","ts":"1710000005.000000","deleted_ts":"1710000001.000000"}}`), slackevents.OptionNoVerifyToken())
	require.NoError(t, err)
	require.NoError(t, client.HandleEventsAPIEvent(ctx, st, "T123", event))
	for _, source := range []string{SourceUser, "mcp"} {
		pending, err := st.PendingThreadWork(ctx, source, "T123", "C123")
		require.NoError(t, err)
		require.Empty(t, pending)
	}
	rows, err := st.QueryReadOnly(ctx, "select ts,deleted_ts from messages")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"ts": "1710000001.000000", "deleted_ts": "1710000001.000000"}}, rows)
	skips, err := st.ListSyncState(ctx, SourceUser, "thread_skip", 20)
	require.NoError(t, err)
	require.Empty(t, skips)
	ordinary := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
		require.NotEqual(t, "/conversations.replies", r.URL.Path, "a tombstoned root must not be fetched to clear a stale skip")
		return primaryOwnerResponse(r.URL.Path), nil
	})
	require.NoError(t, ordinary.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	status, err := st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "full", status.ThreadState)
}

func TestOrdinarySyncReconcilesMergedTombstoneSkips(t *testing.T) {
	for _, marker := range []string{"deleted_ts", "subtype"} {
		t.Run(marker, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			const ts = "1710000001.000000"
			const skipQuery = "select * from sync_state where source_name='api-user' and entity_type='thread_skip'"
			first, replies := true, 0
			now := time.Unix(1710000200, 0).UTC()
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				if first && r.URL.Path == "/conversations.history" {
					return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": ts, "channel": "C123", "thread_ts": ts, "reply_count": 1, "text": "parent"}}}, nil
				}
				if r.URL.Path == "/conversations.replies" {
					replies++
					require.True(t, first, "ordinary reconciliation must not fetch a stored tombstone")
					require.Equal(t, ts, form.Get("ts"))
					return map[string]any{"ok": false, "error": "missing_scope"}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			client.now = func() time.Time { return now }
			// Current-history replies can record a skip during a sliced sync,
			// which deliberately creates no ordinary pending job.
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Since: "1710000000.000000"}))
			require.Equal(t, 1, replies)
			first = false
			before, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			require.Len(t, before, 1)
			pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Empty(t, pending)
			status, err := st.Status(ctx)
			require.NoError(t, err)
			require.Equal(t, "partial", status.ThreadState)

			donor := mustStore(t)
			defer func() { require.NoError(t, donor.Close()) }()
			require.NoError(t, donor.UpsertWorkspace(ctx, store.Workspace{ID: "T123", Name: "Fixture", RawJSON: "{}", UpdatedAt: now}))
			require.NoError(t, donor.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			deleted := store.Message{WorkspaceID: "T123", ChannelID: "C123", TS: ts, ThreadTS: ts, SourceName: SourceUser, SourceRank: 1, RawJSON: "{}", UpdatedAt: now.Add(time.Second)}
			if marker == "deleted_ts" {
				deleted.DeletedTS = "1710000201.000000"
			} else {
				deleted.Subtype = "message_deleted"
			}
			require.NoError(t, donor.UpsertMessage(ctx, deleted, nil))
			opts := share.Options{RepoPath: filepath.Join(t.TempDir(), "share")}
			_, err = share.Export(ctx, donor, opts)
			require.NoError(t, err)
			_, err = share.Import(ctx, st, opts)
			require.NoError(t, err)
			rows, err := st.QueryReadOnly(ctx, "select coalesce(deleted_ts,'') as deleted_ts,coalesce(subtype,'') as subtype,reply_count from messages")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"deleted_ts": deleted.DeletedTS, "subtype": deleted.Subtype, "reply_count": int64(0)}}, rows)
			for _, scoped := range []SyncOptions{
				{WorkspaceID: "T123", Since: "1710000000.000000"},
				{WorkspaceID: "T123", Channels: []string{"COTHER"}},
			} {
				require.NoError(t, client.Sync(ctx, st, scoped))
				after, err := st.QueryReadOnly(ctx, skipQuery)
				require.NoError(t, err)
				require.Equal(t, before, after, "sliced or unselected sync cannot reconcile ordinary work")
			}
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
			after, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			require.Empty(t, after)
			pending, err = st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Empty(t, pending)
			require.Equal(t, 1, replies)
			status, err = st.Status(ctx)
			require.NoError(t, err)
			require.Equal(t, "full", status.ThreadState)
		})
	}
}

func TestRetainedThreadRespectsDMExclusion(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "D123", WorkspaceID: "T123", Name: "dm", Kind: "im", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "D123", WorkspaceID: "T123", TS: "1710000001.000000", ReplyCount: 1, SourceName: SourceUser, SourceRank: 1, RawJSON: "{}", UpdatedAt: now}, nil))
	_, err := st.PrepareThreadWork(ctx, SourceUser, "T123", "D123")
	require.NoError(t, err)
	before, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
	require.NoError(t, err)
	var methods []string
	client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
		methods = append(methods, r.URL.Path)
		if r.URL.Path == "/conversations.list" {
			return map[string]any{"ok": true, "channels": []any{map[string]any{"id": "D123", "is_im": true}}}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	require.Equal(t, []string{"/auth.test", "/auth.test", "/conversations.list", "/users.list"}, methods)
	after, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_pending_v1'")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRetainedThreadPreservesRetentionAndFullRestore(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full=%t", full), func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			retainedOwnerSeed(t, st, "1710000001.000000")
			require.NoError(t, st.SetSyncState(ctx, "retention", "channel_floor", "T123|C123", "1710000010.000000"))
			require.NoError(t, st.SetSyncState(ctx, "retention", "channel_seed", "T123|C123", "1"))
			replies := 0
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path == "/conversations.history" {
					require.Equal(t, map[bool]string{true: "", false: "1710000010.000000"}[full], form.Get("oldest"))
				}
				if r.URL.Path == "/conversations.replies" {
					replies++
					return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "1710000020.000000", "thread_ts": "1710000001.000000", "text": "late reply to old parent"}}}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: full}))
			require.Equal(t, 1, replies)
			rows, err := st.QueryReadOnly(ctx, "select ts from messages order by ts")
			require.NoError(t, err)
			require.Len(t, rows, map[bool]int{true: 2, false: 1}[full])
			pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

func TestRetainedThreadRevocationDuringHTTP(t *testing.T) {
	for _, mode := range []string{"history-delete", "replies-delete", "empty-delete", "error-delete", "invalid-delete", "renewed-full", "complete-full"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			const rootTS = "1710000001.000000"
			retainedOwnerSeed(t, st, rootTS)
			key := "T123|C123|" + rootTS
			if mode == "complete-full" {
				require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy", "old skip"))
			}
			deleteRoot := func() {
				require.NoError(t, st.MarkMessageDeleted(ctx, store.Message{
					WorkspaceID: "T123", ChannelID: "C123", TS: rootTS, DeletedTS: rootTS,
					SourceName: SourceBot, SourceRank: 2, RawJSON: `{"subtype":"message_deleted"}`, UpdatedAt: time.Unix(1710000010, 0).UTC(),
				}, nil))
			}
			calls := 0
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				if r.URL.Path == "/conversations.history" && mode == "history-delete" {
					deleteRoot()
				}
				if r.URL.Path != "/conversations.replies" {
					return primaryOwnerResponse(r.URL.Path), nil
				}
				calls++
				require.Equal(t, 1, calls, "revocation must stop subsequent cursor requests")
				require.Equal(t, rootTS, form.Get("ts"))
				payload := map[string]any{"ok": true, "messages": []any{
					map[string]any{"ts": rootTS, "thread_ts": rootTS, "text": "reply echo"},
					map[string]any{"ts": "1710000003.000000", "thread_ts": rootTS, "text": "reply child"},
				}}
				if mode == "complete-full" {
					return payload, nil
				}
				if mode == "renewed-full" {
					_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceUser, WorkspaceID: "T123", ChannelID: "C123", TS: rootTS}}})
					require.NoError(t, err)
					require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", key, "new attempt skip"))
				} else {
					deleteRoot()
				}
				payload["response_metadata"] = map[string]any{"next_cursor": "second"}
				payload["has_more"] = true
				switch mode {
				case "empty-delete":
					payload["messages"] = []any{}
				case "error-delete":
					payload = map[string]any{"ok": false, "error": "thread_not_found"}
				case "invalid-delete":
					payload["messages"] = []any{map[string]any{"ts": "", "text": "invalid stale reply"}}
				}
				return payload, nil
			})
			full := mode == "renewed-full" || mode == "complete-full"
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: full}))
			require.Equal(t, map[bool]int{true: 0, false: 1}[mode == "history-delete"], calls)
			pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
			require.NoError(t, err)
			require.Len(t, pending, map[bool]int{true: 1, false: 0}[mode == "renewed-full"])
			rows, err := st.QueryReadOnly(ctx, "select ts,coalesce(deleted_ts,'') as deleted_ts,text from messages order by ts")
			require.NoError(t, err)
			if mode == "complete-full" {
				require.Len(t, rows, 2)
			} else {
				require.Len(t, rows, 1, "stale responses must not add the child or overwrite the parent")
				if mode == "renewed-full" {
					require.Equal(t, "retained root", rows[0]["text"])
				} else {
					require.Equal(t, rootTS, rows[0]["deleted_ts"])
				}
			}
			skips, err := st.ListSyncState(ctx, SourceUser, "thread_skip", 20)
			require.NoError(t, err)
			if mode == "renewed-full" {
				require.Equal(t, []store.SyncStateRow{{SourceName: SourceUser, EntityType: "thread_skip", EntityID: key, Value: "new attempt skip"}}, skips)
			} else {
				require.Empty(t, skips)
			}
		})
	}
}

func TestRetainedThreadCachedSkipCannotReplaceNewerWork(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	retainedOwnerSeed(t, st, "1710000001.000000")
	retainedOwnerSeed(t, st, "1710000002.000000")
	const secondKey = "T123|C123|1710000002.000000"
	calls := 0
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		if r.URL.Path != "/conversations.replies" {
			return primaryOwnerResponse(r.URL.Path), nil
		}
		calls++
		require.Equal(t, "1710000001.000000", form.Get("ts"))
		_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceUser, WorkspaceID: "T123", ChannelID: "C123", TS: "1710000002.000000"}}})
		require.NoError(t, err)
		require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", secondKey, "new attempt skip"))
		return map[string]any{"ok": false, "error": "missing_scope"}, nil
	})
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123"}))
	require.Equal(t, 1, calls)
	value, err := st.GetSyncState(ctx, SourceUser, "thread_skip", secondKey)
	require.NoError(t, err)
	require.Equal(t, "new attempt skip", value)
	pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
	require.NoError(t, err)
	require.Len(t, pending, 2)
}

func TestFullThreadSkipCleanupStaysWithinWorkspace(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	const rootTS = "1710000001.000000"
	now := time.Unix(1710000000, 0).UTC()
	for _, id := range []string{"T1", "T2"} {
		require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: id, Name: "Fixture", RawJSON: "{}", UpdatedAt: now}))
		require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: id + "C", WorkspaceID: id, Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	}
	require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "T2C", WorkspaceID: "T2", TS: rootTS, ThreadTS: rootTS, ReplyCount: 1, Text: "retained root", SourceName: SourceBot, SourceRank: 2, RawJSON: "{}", UpdatedAt: now}, nil))
	_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceUser, WorkspaceID: "T2", ChannelID: "T2C", TS: rootTS}}})
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T1|legacy", "obsolete skip"))
	require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T2|T2C|"+rootTS, "pending skip"))
	const t2StateQuery = `select * from sync_state where source_name = 'api-user'
and entity_id in ('["T2","T2C","1710000001.000000"]', 'T2|T2C|1710000001.000000')
order by entity_type, entity_id`
	before, err := st.QueryReadOnly(ctx, t2StateQuery)
	require.NoError(t, err)
	require.Len(t, before, 2)
	replyCalls := map[string]int{}
	clientFor := func(workspaceID string) *Client {
		return primaryOwnerClient(t, config.Tokens{User: "fixture-user-" + workspaceID}, func(r *http.Request, form url.Values) (any, error) {
			switch r.URL.Path {
			case "/auth.test":
				return map[string]any{"ok": true, "team_id": workspaceID, "team": "Fixture"}, nil
			case "/conversations.list":
				return map[string]any{"ok": true, "channels": []any{map[string]any{"id": workspaceID + "C", "name": "fixture", "is_channel": true}}}, nil
			case "/conversations.history":
				require.Equal(t, workspaceID+"C", form.Get("channel"))
				return primaryOwnerResponse(r.URL.Path), nil
			case "/conversations.replies":
				replyCalls[workspaceID]++
				require.Equal(t, "T2", workspaceID)
				require.Equal(t, "T2C", form.Get("channel"))
				require.Equal(t, rootTS, form.Get("ts"))
				return map[string]any{"ok": true, "messages": []any{
					map[string]any{"ts": rootTS, "thread_ts": rootTS, "text": "reply echo"},
					map[string]any{"ts": "1710000003.000000", "thread_ts": rootTS, "text": "reply child"},
				}}, nil
			default:
				return primaryOwnerResponse(r.URL.Path), nil
			}
		})
	}
	require.NoError(t, clientFor("T1").Sync(ctx, st, SyncOptions{WorkspaceID: "T1", Full: true}))
	t1Skips, err := st.QueryReadOnly(ctx, "select * from sync_state where source_name='api-user' and entity_type='thread_skip' and entity_id like 'T1|%'")
	require.NoError(t, err)
	require.Empty(t, t1Skips)
	after, err := st.QueryReadOnly(ctx, t2StateQuery)
	require.NoError(t, err)
	require.Equal(t, before, after, "other workspace generations, skips and updated_at must remain unchanged")
	status, err := st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "partial", status.ThreadState)
	require.Empty(t, replyCalls)

	require.NoError(t, clientFor("T2").Sync(ctx, st, SyncOptions{WorkspaceID: "T2"}))
	require.Equal(t, map[string]int{"T2": 1}, replyCalls)
	after, err = st.QueryReadOnly(ctx, t2StateQuery)
	require.NoError(t, err)
	require.Empty(t, after)
	status, err = st.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "full", status.ThreadState)
}

func retainedOwnerSeed(t *testing.T, st *store.Store, ts string) {
	t.Helper()
	now := time.Unix(1710000000, 0).UTC()
	require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: "C123", WorkspaceID: "T123", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertMessage(context.Background(), store.Message{ChannelID: "C123", WorkspaceID: "T123", TS: ts, ThreadTS: ts, Text: "retained root", NormalizedText: "retained root", ReplyCount: 1, SourceName: SourceBot, SourceRank: 2, RawJSON: "{}", UpdatedAt: now}, nil))
}
