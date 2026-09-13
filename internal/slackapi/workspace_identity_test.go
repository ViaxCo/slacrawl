package slackapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestSyncRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	for _, workspaceID := range []string{"", "T123"} {
		t.Run("requested="+workspaceID, func(t *testing.T) {
			server := newMismatchedUserSlackServer(t)
			defer server.Close()
			client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			st := mustStore(t)
			defer func() { require.NoError(t, st.Close()) }()
			err := client.Sync(context.Background(), st, SyncOptions{WorkspaceID: workspaceID})
			require.ErrorContains(t, err, "user token: authenticated workspace T999 does not match requested workspace T123")
			require.Equal(t, 2, server.calls("auth.test"))
			assertNoWorkspaceDataRequests(t, server)
			assertEmptyWorkspaceArchive(t, st)
		})
	}
}

func TestRepairRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	server := newMismatchedUserSlackServer(t)
	defer server.Close()
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	err := client.repairWorkspace(context.Background(), st, "T123")
	require.ErrorContains(t, err, "user token: authenticated workspace T999 does not match requested workspace T123")
	require.Equal(t, 1, server.calls("auth.test"))
	assertNoWorkspaceDataRequests(t, server)
	assertEmptyWorkspaceArchive(t, st)
}

func TestDoctorRejectsSecondaryWorkspaceMismatch(t *testing.T) {
	server := newMismatchedUserSlackServer(t)
	defer server.Close()
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test", User: "xoxp-test"}, server.URL()+"/", server.Client())
	diag, err := client.Doctor(context.Background())
	require.NoError(t, err)
	require.True(t, diag.UserConfigured)
	require.Equal(t, "T123", diag.BotAuthTeamID)
	require.False(t, diag.UserAuthAvailable)
	require.Equal(t, "partial", diag.ThreadCoverage)
	require.False(t, diag.DMsIncluded)
	require.Empty(t, diag.DMsMissingScope)
	require.Contains(t, diag.UserAuthError, "authenticated workspace T999 does not match requested workspace T123")
	require.Equal(t, 2, server.calls("auth.test"))
	assertNoWorkspaceDataRequests(t, server)
}

func assertNoWorkspaceDataRequests(t *testing.T, server *mockSlackServer) {
	t.Helper()
	for _, endpoint := range []string{"conversations.list", "conversations.history", "conversations.replies", "users.list"} {
		require.Zero(t, server.calls(endpoint), endpoint)
	}
}

func assertEmptyWorkspaceArchive(t *testing.T, st *store.Store) {
	t.Helper()
	for _, table := range []string{
		"workspaces", "channels", "users", "messages", "message_files", "message_events",
		"message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state",
	} {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		require.Empty(t, rows, table)
	}
}

func newMismatchedUserSlackServer(t *testing.T) *mockSlackServer {
	t.Helper()
	mock := &mockSlackServer{counts: map[string]int{}}
	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		mock.counts[r.URL.Path]++
		mock.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			teamID := "T123"
			if mustFormValues(r).Get("token") == "xoxp-test" || strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == "xoxp-test" {
				teamID = "T999"
			}
			// The same Enterprise Grid organization does not make teams interchangeable.
			_, _ = fmt.Fprintf(w, `{"ok":true,"team_id":%q,"enterprise_id":"E123"}`, teamID)
		case "/conversations.list":
			_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C123","name":"general","is_channel":true}],"response_metadata":{"next_cursor":""}}`))
		case "/conversations.history":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","text":"root","ts":"1710000000.000100","reply_count":1}],"response_metadata":{"next_cursor":""}}`))
		case "/conversations.replies":
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","text":"foreign-workspace-reply","ts":"1710000001.000100","thread_ts":"1710000000.000100"}],"response_metadata":{"next_cursor":""}}`))
		case "/users.list":
			_, _ = w.Write([]byte(`{"ok":true,"members":[],"response_metadata":{"next_cursor":""}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	return mock
}
