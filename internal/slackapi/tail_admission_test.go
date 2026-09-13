package slackapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestTailExcludesTypedDMsBeforePersistenceAndAcks(t *testing.T) {
	for _, kind := range []string{"im", "mpim", "app_home"} {
		for _, subtype := range []string{"", "message_changed", "message_deleted", "reply", "thread_broadcast"} {
			t.Run(kind+"/"+subtype, func(t *testing.T) {
				server := admissionServer(t, nil, nil)
				defer server.Close()
				client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
				st := mustStore(t)
				defer st.Close()
				socket := &fakeSocketMode{}
				err := handleTailFixture(t, client, st, socket, "T123", tailMessageFixture(kind, subtype))
				require.NoError(t, err)
				require.Equal(t, 1, socket.acks)
				require.Zero(t, server.calls("conversations.info"))
				assertEmptyWorkspaceArchive(t, st)
			})
		}
	}
}

func TestTailPreservesDefaultsAndNativeChannelEvents(t *testing.T) {
	for _, tc := range []struct {
		policy admission.DMPolicy
		kind   string
	}{
		{admission.Default, "im"}, {admission.Default, "mpim"}, {admission.Default, "app_home"},
		{admission.Include, "im"}, {admission.Include, "mpim"}, {admission.Include, "app_home"},
		{admission.Exclude, "channel"}, {admission.Exclude, "group"},
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.policy, tc.kind), func(t *testing.T) {
			server := admissionServer(t, nil, nil)
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(tc.policy)
			st := mustStore(t)
			defer st.Close()
			socket := &fakeSocketMode{}
			require.NoError(t, handleTailFixture(t, client, st, socket, "T123", tailMessageFixture(tc.kind, "")))
			require.Equal(t, 1, socket.acks)
			require.Zero(t, server.calls("conversations.info"))
			rows, err := st.Messages(context.Background(), "T123", "C123", "", 10)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Contains(t, rows[0].Text, "tail-content-canary")
		})
	}
}

func TestTailLookupAdmissionBeforeAck(t *testing.T) {
	for _, tc := range []struct {
		name    string
		channel map[string]any
		errCode string
		status  int
		want    string
	}{
		{"public", map[string]any{"id": "C123", "is_channel": true}, "", 200, "write"},
		{"private", map[string]any{"id": "C123", "is_channel": true, "is_private": true}, "", 200, "write"},
		{"legacy private", map[string]any{"id": "C123", "is_group": true, "is_private": true}, "", 200, "write"},
		{"im", map[string]any{"id": "C123", "is_im": true}, "", 200, "skip"},
		{"mpim", map[string]any{"id": "C123", "is_mpim": true}, "", 200, "skip"},
		{"im conflicting channel", map[string]any{"id": "C123", "is_im": true, "is_channel": true}, "", 200, "skip"},
		{"wrong id", map[string]any{"id": "COTHER", "is_channel": true}, "", 200, "error"},
		{"empty id", map[string]any{"is_channel": true}, "", 200, "error"},
		{"context mismatch", map[string]any{"id": "C123", "is_channel": true, "context_team_id": "TOTHER"}, "", 200, "error"},
		{"unknown", map[string]any{"id": "C123", "is_private": true}, "", 200, "error"},
		{"conflicting", map[string]any{"id": "C123", "is_channel": true, "is_group": true}, "", 200, "error"},
		{"missing scope", nil, "missing_scope", 200, "error"},
		{"not found", nil, "channel_not_found", 200, "error"},
		{"http error", nil, "", 500, "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := admissionServer(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
				require.Equal(t, "/conversations.info", r.URL.Path)
				require.Equal(t, "C123", r.Form.Get("channel"))
				require.Equal(t, "false", r.Form.Get("include_locale"))
				require.Equal(t, "false", r.Form.Get("include_num_members"))
				require.Equal(t, "fixture", r.Form.Get("token"))
				w.WriteHeader(tc.status)
				if tc.errCode != "" {
					writeAdmissionJSON(t, w, map[string]any{"ok": false, "error": tc.errCode})
				} else {
					if tc.channel != nil {
						tc.channel["latest"] = admissionMessage("lookup-content-canary", "DOTHER", "1700000000.000000")
					}
					writeAdmissionJSON(t, w, map[string]any{"ok": true, "channel": tc.channel})
				}
				return true
			})
			defer server.Close()
			var logs bytes.Buffer
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude).WithLogger(testProgressLogger(&logs))
			st := mustStore(t)
			defer st.Close()
			// A prior/default public kind is deliberately not classification proof.
			require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: "C123", WorkspaceID: "T123", Kind: "public_channel", Name: "prior", UpdatedAt: time.Unix(1710000000, 0)}))
			socket := &fakeSocketMode{}
			err := handleTailFixture(t, client, st, socket, "T123", tailMessageFixture("", "message_changed"))
			if tc.want == "error" {
				require.Error(t, err)
				require.Zero(t, socket.acks)
				logs.WriteString(err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, socket.acks)
			}
			require.Equal(t, 1, server.calls("conversations.info"))
			rows, err := st.Messages(context.Background(), "T123", "C123", "", 10)
			require.NoError(t, err)
			if tc.want == "write" {
				require.Len(t, rows, 1)
			} else {
				require.Empty(t, rows)
				assertAdmissionCanariesAbsent(t, st, logs.String(), "tail-content-canary")
			}
			assertAdmissionCanariesAbsent(t, st, logs.String(), "lookup-content-canary", "DOTHER")
		})
	}
}

func TestTailUntypedEventsResolveAgain(t *testing.T) {
	var lookups atomic.Int32
	server := admissionServer(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		require.Equal(t, "/conversations.info", r.URL.Path)
		channel := map[string]any{"id": "C123", "is_channel": true}
		if lookups.Add(1) == 2 {
			channel = map[string]any{"id": "C123", "is_im": true}
		}
		writeAdmissionJSON(t, w, map[string]any{"ok": true, "channel": channel})
		return true
	})
	defer server.Close()
	client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
	st := mustStore(t)
	defer st.Close()
	socket := &fakeSocketMode{}
	require.NoError(t, handleTailFixture(t, client, st, socket, "T123", tailMessageFixture("unrecognized", "")))
	before := make(map[string][]map[string]any)
	for _, table := range admissionTables {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		before[table] = rows
	}
	second := tailMessageFixture("", "message_deleted")
	require.NoError(t, handleTailFixture(t, client, st, socket, "T123", second))
	for _, table := range admissionTables {
		after, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		require.Equal(t, before[table], after, "a prior positive lookup cannot authorize the later deletion: "+table)
	}
	require.Equal(t, int32(2), lookups.Load())
	require.Equal(t, 2, socket.acks)
}

func TestTailRejectsContradictoryEnvelopeIdentity(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, field := range []string{"channel", "message", "previous_message", "root"} {
			t.Run(fmt.Sprintf("%d/%s", policy, field), func(t *testing.T) {
				server := admissionServer(t, nil, nil)
				defer server.Close()
				client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(policy)
				st := mustStore(t)
				defer st.Close()
				inner := tailMessageFixture("channel", "message_changed")
				switch field {
				case "channel":
					inner["channel"] = ""
				default:
					inner[field] = admissionMessage("identity-content-canary", "COTHER", "1700000000.000000")
				}
				socket := &fakeSocketMode{}
				err := handleTailFixture(t, client, st, socket, "T123", inner)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "content-canary")
				require.Zero(t, socket.acks)
				require.Zero(t, server.calls("conversations.info"))
				assertEmptyWorkspaceArchive(t, st)
			})
		}
	}
}

func TestTailAcceptsSharedWorkspaceEnvelope(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			server := admissionServer(t, nil, nil)
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(policy)
			st := mustStore(t)
			defer st.Close()
			inner := tailMessageFixture("channel", "message_changed")
			inner["ts"] = "1720000000.000000"
			inner["team"] = "THOST"
			inner["source_team"] = "TAUTHOR"
			inner["user_team"] = "TAUTHOR"
			inner["message"].(map[string]any)["team"] = "TAUTHOR"
			socket := &fakeSocketMode{}
			require.NoError(t, handleTailFixture(t, client, st, socket, "TSHARED", inner))
			require.Equal(t, 1, socket.acks)
			require.Zero(t, server.calls("conversations.info"))
			rows, err := st.Messages(context.Background(), "T123", "C123", "", 10)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "1710000000.000000", rows[0].TS)
		})
	}
}

func TestTailLookupCancellationDoesNotAck(t *testing.T) {
	for _, rateLimited := range []bool{false, true} {
		t.Run(fmt.Sprintf("rate_limited=%v", rateLimited), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			server := admissionServer(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
				require.Equal(t, "/conversations.info", r.URL.Path)
				if rateLimited {
					w.Header().Set("Retry-After", "60")
					w.WriteHeader(http.StatusTooManyRequests)
				} else {
					close(started)
					<-r.Context().Done()
				}
				return true
			})
			defer func() { cancel(); server.Close() }()
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			if rateLimited {
				client.sleep = func(ctx context.Context, delay time.Duration) error {
					require.Equal(t, time.Minute, delay)
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}
			}
			st := mustStore(t)
			defer st.Close()
			socket := &fakeSocketMode{}
			event := tailFixtureEvent(t, "T123", tailMessageFixture("", "message_changed"))
			result := make(chan error, 1)
			go func() { result <- client.handleSocketModeEvent(ctx, st, "T123", socket, event) }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("lookup did not reach the cancellation barrier")
			}
			cancel()
			select {
			case err := <-result:
				require.True(t, errors.Is(err, context.Canceled), "%v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("lookup did not return after cancellation")
			}
			require.Zero(t, socket.acks)
			require.Equal(t, 1, server.calls("conversations.info"))
			assertEmptyWorkspaceArchive(t, st)
		})
	}
}

func TestTailMetadataAdmissionAndWorkspaceOwnership(t *testing.T) {
	for _, operation := range []string{"channel_rename", "channel_archive", "channel_unarchive"} {
		for _, tc := range []struct {
			name, owner, kind string
			policy            admission.DMPolicy
			want              string
		}{
			{"default owned", "T123", "", admission.Default, "write"},
			{"default foreign", "TOTHER", "", admission.Default, "skip"},
			{"include owned", "T123", "", admission.Include, "write"},
			{"include foreign", "TOTHER", "", admission.Include, "skip"},
			{"exclude public", "T123", "public", admission.Exclude, "write"},
			{"exclude DM", "T123", "im", admission.Exclude, "skip"},
			{"exclude unknown", "T123", "unknown", admission.Exclude, "error"},
			{"exclude foreign", "TOTHER", "", admission.Exclude, "skip"},
			{"exclude missing", "", "", admission.Exclude, "skip"},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				server := admissionServer(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
					require.Equal(t, "/conversations.info", r.URL.Path)
					channel := map[string]any{"id": "C123", "latest": admissionMessage("lookup-content-canary", "DOTHER", "1.0")}
					if tc.kind == "public" {
						channel["is_channel"] = true
					} else if tc.kind == "im" {
						channel["is_im"] = true
					}
					writeAdmissionJSON(t, w, map[string]any{"ok": true, "channel": channel})
					return true
				})
				defer server.Close()
				client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(tc.policy)
				st := mustStore(t)
				defer st.Close()
				if tc.owner != "" {
					require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: "C123", WorkspaceID: tc.owner, Name: "original", IsArchived: operation == "channel_unarchive", UpdatedAt: time.Unix(1710000000, 0)}))
				}
				before, err := st.QueryReadOnly(context.Background(), "select * from channels")
				require.NoError(t, err)
				inner := map[string]any{"type": operation, "channel": "C123"}
				if operation == "channel_rename" {
					inner["channel"] = map[string]any{"id": "C123", "name": "metadata-content-canary"}
				}
				socket := &fakeSocketMode{}
				err = handleTailFixture(t, client, st, socket, "T123", inner)
				if tc.want == "error" {
					require.Error(t, err)
					require.Zero(t, socket.acks)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, socket.acks)
				}
				after, err := st.QueryReadOnly(context.Background(), "select * from channels")
				require.NoError(t, err)
				if tc.want == "write" {
					require.NotEqual(t, before, after)
				} else {
					require.Equal(t, before, after)
					assertAdmissionCanariesAbsent(t, st, "", "metadata-content-canary")
				}
				wantLookups := 0
				if tc.policy == admission.Exclude && tc.owner == "T123" {
					wantLookups = 1
				}
				require.Equal(t, wantLookups, server.calls("conversations.info"))
				assertAdmissionCanariesAbsent(t, st, "", "lookup-content-canary", "DOTHER")
			})
		}
	}
}

func handleTailFixture(t *testing.T, client *Client, st *store.Store, socket *fakeSocketMode, team string, inner map[string]any) error {
	t.Helper()
	return client.handleSocketModeEvent(context.Background(), st, "T123", socket, tailFixtureEvent(t, team, inner))
}

func tailFixtureEvent(t *testing.T, team string, inner map[string]any) socketmode.Event {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"type": "event_callback", "team_id": team, "event": inner})
	require.NoError(t, err)
	event, err := slackevents.ParseEvent(raw, slackevents.OptionNoVerifyToken())
	require.NoError(t, err)
	return socketmode.Event{Type: socketmode.EventTypeEventsAPI, Data: event, Request: &socketmode.Request{EnvelopeID: "fixture", Type: "events_api"}}
}

func tailMessageFixture(kind, subtype string) map[string]any {
	inner := admissionMessage("tail-content-canary", "C123", "1710000000.000000")
	inner["channel_type"] = kind
	switch subtype {
	case "message_changed":
		inner["subtype"] = subtype
		inner["message"] = admissionMessage("tail-content-canary", "C123", "1710000000.000000")
		inner["previous_message"] = admissionMessage("tail-content-canary", "C123", "1710000000.000000")
	case "message_deleted":
		inner["subtype"] = subtype
		inner["deleted_ts"] = "1710000000.000000"
		inner["previous_message"] = admissionMessage("tail-content-canary", "C123", "1710000000.000000")
	case "thread_broadcast":
		inner["subtype"] = subtype
		inner["root"] = admissionMessage("tail-content-canary", "C123", "1700000000.000000")
		inner["thread_ts"] = "1700000000.000000"
	case "reply":
		inner["thread_ts"] = "1700000000.000000"
	}
	return inner
}
