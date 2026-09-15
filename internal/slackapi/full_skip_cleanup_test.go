package slackapi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestFullThreadSkipCleanupRequiresUnrestrictedScope(t *testing.T) {
	const since = "1710000000.000000"
	const root = "1710000001.000000"
	for _, tc := range []struct {
		name         string
		opts         SyncOptions
		policy       admission.DMPolicy
		dm           bool
		histories    int
		replies      int
		clearsLegacy bool
	}{
		{name: "since-beyond-root", opts: SyncOptions{Full: true, Since: "1710000010.000000"}, policy: admission.Include, histories: 2},
		{name: "excluded-id", opts: SyncOptions{Full: true, ExcludeChannels: []string{"C123"}}, policy: admission.Include, histories: 1},
		{name: "excluded-name", opts: SyncOptions{Full: true, ExcludeChannels: []string{" #FiXtUrE "}}, policy: admission.Include, histories: 1},
		{name: "allowlist", opts: SyncOptions{Full: true, Channels: []string{"COTHER"}}, policy: admission.Include, histories: 1},
		{name: "dms-excluded", opts: SyncOptions{Full: true}, policy: admission.Exclude, dm: true, histories: 1},
		{name: "scoped-replies-complete", opts: SyncOptions{Full: true, Since: since}, policy: admission.Include, histories: 2, replies: 1},
		{name: "unmatched-exclusion", opts: SyncOptions{Full: true, ExcludeChannels: []string{"absent"}}, policy: admission.Include, histories: 2, replies: 1},
		{name: "full-default", opts: SyncOptions{Full: true}, policy: admission.Default, histories: 2, replies: 1, clearsLegacy: true},
		{name: "full-precedes-latest-only", opts: SyncOptions{Full: true, LatestOnly: true}, policy: admission.Include, histories: 2, replies: 1, clearsLegacy: true},
		{name: "empty-exclusions", opts: SyncOptions{Full: true, ExcludeChannels: []string{" # ", " "}}, policy: admission.Include, histories: 2, replies: 1, clearsLegacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			channelID := "C123"
			channel := map[string]any{"id": channelID, "name": "fixture", "is_channel": true}
			if tc.dm {
				channelID = "D123"
				channel = map[string]any{"id": channelID, "is_im": true, "is_private": true, "user": "U123"}
			}
			phase := 0
			var histories, replies [2]int
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				switch r.URL.Path {
				case "/conversations.list":
					channels := []any{}
					if form.Get("types") == "im,mpim" {
						if tc.dm {
							channels = append(channels, channel)
						}
					} else {
						channels = append(channels, map[string]any{"id": "COTHER", "name": "other", "is_channel": true})
						if !tc.dm {
							channels = append(channels, channel)
						}
					}
					return map[string]any{"ok": true, "channels": channels}, nil
				case "/conversations.history":
					histories[phase]++
					wantOldest := since
					if phase == 1 {
						wantOldest = tc.opts.Since
					}
					require.Equal(t, wantOldest, form.Get("oldest"))
					require.Contains(t, []string{channelID, "COTHER"}, form.Get("channel"))
					if form.Get("channel") == channelID && (phase == 0 || tc.opts.Since == since) {
						return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": root, "thread_ts": root, "reply_count": 1, "text": "retained root"}}}, nil
					}
					return primaryOwnerResponse(r.URL.Path), nil
				case "/conversations.replies":
					replies[phase]++
					require.Equal(t, channelID, form.Get("channel"))
					require.Equal(t, root, form.Get("ts"))
					if phase == 0 {
						return map[string]any{"ok": false, "error": "missing_scope"}, nil
					}
					return map[string]any{"ok": true, "messages": []any{
						map[string]any{"ts": root, "thread_ts": root, "text": "retained root"},
						map[string]any{"ts": "1710000002.000000", "thread_ts": root, "text": "recovered reply"},
					}}, nil
				default:
					return primaryOwnerResponse(r.URL.Path), nil
				}
			}).WithDMPolicy(admission.Include)
			client.now = func() time.Time { return time.Unix(1710000100, 0).UTC() }
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Since: since}))
			require.Equal(t, 2, histories[0])
			require.Equal(t, 1, replies[0])
			skips, err := st.ListSyncState(ctx, SourceUser, "thread_skip", 10)
			require.NoError(t, err)
			require.Equal(t, []store.SyncStateRow{{SourceName: SourceUser, EntityType: "thread_skip", EntityID: "T123|" + channelID + "|" + root, Value: "missing_scope"}}, skips)
			pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", channelID)
			require.NoError(t, err)
			require.Empty(t, pending, "a sliced reply failure creates a real skip-only state")
			coverage, err := st.GetSyncState(ctx, "doctor", "threads", "coverage")
			require.NoError(t, err)
			require.Equal(t, "partial", coverage)
			require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy", "unknown origin"))
			const skipQuery = "select * from sync_state where entity_type='thread_skip' order by entity_id"
			before, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			phase = 1
			client.WithDMPolicy(tc.policy)
			tc.opts.WorkspaceID = "T123"
			require.NoError(t, client.Sync(ctx, st, tc.opts))
			require.Equal(t, tc.histories, histories[1])
			require.Equal(t, tc.replies, replies[1])
			after, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			switch {
			case tc.clearsLegacy:
				require.Empty(t, after)
			case tc.replies == 1:
				require.Equal(t, before[1:], after, "successful scoped replies clear only their own skip")
			default:
				require.Equal(t, before, after, "unvisited skips retain their exact values and timestamps")
			}
			pending, err = st.PendingThreadWork(ctx, SourceUser, "T123", channelID)
			require.NoError(t, err)
			require.Empty(t, pending)
			rows, err := st.QueryReadOnly(ctx, "select ts,text from messages order by ts")
			require.NoError(t, err)
			require.Len(t, rows, 1+tc.replies)
			if tc.replies == 1 {
				require.Equal(t, "recovered reply", rows[1]["text"])
			}
			coverage, err = st.GetSyncState(ctx, "doctor", "threads", "coverage")
			require.NoError(t, err)
			require.Equal(t, map[bool]string{true: "full", false: "partial"}[tc.clearsLegacy], coverage)
		})
	}
}

func TestFullThreadSkipCleanupRecordsActualOmissions(t *testing.T) {
	for _, mode := range []string{"dm-list-scope", "dm-list-filter", "admission-veto", "history-not-found", "history-not-member", "dm-history-scope", "channel-collision", "concurrent-channel-collision", "history-collision", "reply-collision"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			now := time.Unix(1710000100, 0).UTC()
			const root = "1710000001.000000"
			const child = "1710000002.000000"
			if mode == "channel-collision" || mode == "concurrent-channel-collision" {
				require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TOTHER", Name: "foreign channel canary", Kind: "public_channel", RawJSON: `{"canary":"foreign channel"}`, UpdatedAt: now}))
			}
			if mode == "history-collision" || mode == "reply-collision" {
				ts := root
				if mode == "reply-collision" {
					ts = child
				}
				require.NoError(t, st.UpsertMessage(ctx, store.Message{WorkspaceID: "TOTHER", ChannelID: "C123", TS: ts, Text: "foreign message canary", SourceName: SourceBot, SourceRank: 2, RawJSON: `{"canary":"foreign message"}`, UpdatedAt: now}, nil))
			}
			foreignChannels, err := st.QueryReadOnly(ctx, "select * from channels where workspace_id='TOTHER'")
			require.NoError(t, err)
			foreignMessages, err := st.QueryReadOnly(ctx, "select * from messages where workspace_id='TOTHER'")
			require.NoError(t, err)
			var mu sync.Mutex
			calls := map[string]int{}
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				mu.Lock()
				calls[r.URL.Path+"|"+form.Get("channel")]++
				mu.Unlock()
				switch r.URL.Path {
				case "/conversations.list":
					if form.Get("types") == "im,mpim" {
						switch mode {
						case "dm-list-scope":
							return map[string]any{"ok": false, "error": "missing_scope"}, nil
						case "dm-list-filter":
							return primaryOwnerResponse(r.URL.Path), nil
						case "dm-history-scope":
							return map[string]any{"ok": true, "channels": []any{map[string]any{"id": "D123", "is_im": true, "is_private": true}}}, nil
						default:
							return map[string]any{"ok": true, "channels": []any{}}, nil
						}
					}
					channels := []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true}, map[string]any{"id": "COK", "name": "readable", "is_channel": true}}
					if mode == "admission-veto" {
						channels = append(channels, map[string]any{"id": "D123", "is_im": true, "is_private": true, "latest": map[string]any{"ts": child, "text": "excluded DM canary"}})
					}
					return map[string]any{"ok": true, "channels": channels}, nil
				case "/conversations.history":
					channel := form.Get("channel")
					if channel == "D123" && mode == "dm-history-scope" {
						return map[string]any{"ok": false, "error": "missing_scope"}, nil
					}
					if channel == "C123" {
						switch mode {
						case "history-not-found":
							return map[string]any{"ok": false, "error": "channel_not_found"}, nil
						case "history-not-member":
							return map[string]any{"ok": false, "error": "not_in_channel"}, nil
						case "history-collision", "reply-collision":
							return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": root, "thread_ts": root, "reply_count": 1, "text": "incoming root"}}}, nil
						default:
							return primaryOwnerResponse(r.URL.Path), nil
						}
					}
					if channel == "COK" {
						return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": root, "thread_ts": root, "reply_count": 1, "text": "readable root"}}}, nil
					}
					return nil, fmt.Errorf("unexpected fixture history channel")
				case "/conversations.replies":
					if form.Get("ts") != root || (form.Get("channel") != "COK" && !(mode == "reply-collision" && form.Get("channel") == "C123")) {
						return nil, fmt.Errorf("unexpected fixture replies tuple")
					}
					return map[string]any{"ok": true, "messages": []any{
						map[string]any{"ts": root, "thread_ts": root, "text": "readable root"},
						map[string]any{"ts": child, "thread_ts": root, "text": "readable reply"},
					}}, nil
				default:
					return primaryOwnerResponse(r.URL.Path), nil
				}
			}).WithDMPolicy(admission.Include)
			client.now = func() time.Time { return now }
			if mode == "admission-veto" {
				client.WithDMPolicy(admission.Exclude)
			}
			opts := SyncOptions{WorkspaceID: "T123", Full: true, Concurrency: 2}
			if mode == "channel-collision" {
				opts.Concurrency = 1
			}
			require.NoError(t, st.SetSyncState(ctx, "doctor", "threads", "coverage", "full"))
			for attempt := 1; attempt <= 2; attempt++ {
				if attempt == 2 {
					require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy", "unknown origin"))
				}
				before, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_skip'")
				require.NoError(t, err)
				require.NoError(t, client.Sync(ctx, st, opts))
				after, err := st.QueryReadOnly(ctx, "select * from sync_state where entity_type='thread_skip'")
				require.NoError(t, err)
				require.Equal(t, before, after, "omitted work cannot erase unrelated legacy diagnostics")
				pending, err := st.ListSyncState(ctx, SourceUser, store.ThreadPendingEntityType, 10)
				require.NoError(t, err)
				require.Empty(t, pending, "the omission veto must not depend on retained pending work")
				coverage, err := st.GetSyncState(ctx, "doctor", "threads", "coverage")
				require.NoError(t, err)
				require.Equal(t, "partial", coverage, "an actual omission cannot newly claim full, even without a prior skip")
				mu.Lock()
				gotCalls := make(map[string]int, len(calls))
				for key, count := range calls {
					gotCalls[key] = count
				}
				mu.Unlock()
				require.Equal(t, attempt, gotCalls["/conversations.replies|COK"], "omission facts must not enter the replies skip cache")
				require.Equal(t, map[bool]int{true: attempt, false: 0}[mode == "reply-collision"], gotCalls["/conversations.replies|C123"])
				require.Zero(t, gotCalls["/conversations.replies|D123"])
				if mode == "channel-collision" || mode == "concurrent-channel-collision" {
					require.Zero(t, gotCalls["/conversations.history|C123"])
				}
				if mode == "admission-veto" {
					require.Zero(t, gotCalls["/conversations.history|D123"])
					rows, err := st.QueryReadOnly(ctx, "select * from channels where id='D123'")
					require.NoError(t, err)
					require.Empty(t, rows)
				}
				afterChannels, err := st.QueryReadOnly(ctx, "select * from channels where workspace_id='TOTHER'")
				require.NoError(t, err)
				require.Equal(t, foreignChannels, afterChannels)
				afterMessages, err := st.QueryReadOnly(ctx, "select * from messages where workspace_id='TOTHER'")
				require.NoError(t, err)
				require.Equal(t, foreignMessages, afterMessages)
				rows, err := st.QueryReadOnly(ctx, "select text from messages where channel_id='COK' order by ts")
				require.NoError(t, err)
				require.Equal(t, []map[string]any{{"text": "readable root"}, {"text": "readable reply"}}, rows)
			}
		})
	}
}

func TestScopedThreadCoverageWithoutOmissions(t *testing.T) {
	for _, opts := range []SyncOptions{
		{Since: "1710000000.000000"},
		{Full: true, Since: "1710000000.000000"},
		{Full: true, Channels: []string{"C123"}},
		{Full: true, ExcludeChannels: []string{"absent"}},
		{Full: true},
	} {
		t.Run(fmt.Sprintf("%+v", opts), func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, _ url.Values) (any, error) {
				return primaryOwnerResponse(r.URL.Path), nil
			})
			opts.WorkspaceID = "T123"
			require.NoError(t, client.Sync(ctx, st, opts))
			coverage, err := st.GetSyncState(ctx, "doctor", "threads", "coverage")
			require.NoError(t, err)
			require.Equal(t, "full", coverage, "static scope restrictions alone do not redefine existing scoped coverage")
		})
	}
}

func TestFullThreadSkipCleanupAfterJoin(t *testing.T) {
	for _, joins := range []bool{false, true} {
		t.Run(fmt.Sprintf("join-succeeds=%t", joins), func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|legacy", "unknown origin"))
			const skipQuery = "select * from sync_state where entity_type='thread_skip'"
			before, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			histories, joinCalls := 0, 0
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				switch r.URL.Path {
				case "/conversations.list":
					if form.Get("types") == "im,mpim" {
						return map[string]any{"ok": true, "channels": []any{}}, nil
					}
				case "/conversations.history":
					histories++
					require.Equal(t, "C123", form.Get("channel"))
					if histories == 1 {
						return map[string]any{"ok": false, "error": "not_in_channel"}, nil
					}
				case "/conversations.join":
					joinCalls++
					require.Equal(t, "C123", form.Get("channel"))
					if !joins {
						return map[string]any{"ok": false, "error": "restricted_action"}, nil
					}
					return map[string]any{"ok": true}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			}).WithDMPolicy(admission.Include)
			require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true}))
			require.Equal(t, 1, joinCalls)
			require.Equal(t, map[bool]int{true: 2, false: 1}[joins], histories)
			after, err := st.QueryReadOnly(ctx, skipQuery)
			require.NoError(t, err)
			if joins {
				require.Empty(t, after, "a recovered history request is not omitted work")
			} else {
				require.Equal(t, before, after)
			}
			joinState, err := st.GetSyncState(ctx, SourceBot, "channel_join", "C123")
			require.NoError(t, err)
			require.Equal(t, map[bool]string{true: "joined", false: "failed:restricted_action"}[joins], joinState)
			coverage, err := st.GetSyncState(ctx, "doctor", "threads", "coverage")
			require.NoError(t, err)
			require.Equal(t, map[bool]string{true: "full", false: "partial"}[joins], coverage)
		})
	}
}
