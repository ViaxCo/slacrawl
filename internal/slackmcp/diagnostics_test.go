package slackmcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const diagnosticCanary = "SYNTHETIC_PRIVATE_MCP_CONTENT"
const diagnosticNumber = "867530912345678901234567890"

func TestParserDiagnosticsExcludeResponseContent(t *testing.T) {
	channels := func(raw string) error { _, err := parseChannels(raw); return err }
	users := func(raw string) error { _, err := parseUsers(raw); return err }
	messages := func(raw string) error { _, err := parseChannelMessages(raw); return err }
	thread := func(raw string) error { _, err := parseThreadMessages(raw, "C123"); return err }
	reference := func(raw string) error { return decodeReferenceResponse(raw, &referenceResponse{}) }
	for _, tc := range []struct {
		name, raw, want string
		parse           func(string) error
	}{
		{"channel JSON", `{"results":` + diagnosticNumber + `}`, "channel search payload: invalid response", channels},
		{"user JSON", `{"results":` + diagnosticNumber + `}`, "user search payload: invalid response", users},
		{"message JSON", `{"messages":` + diagnosticNumber + `}`, "channel payload: invalid response", messages},
		{"thread JSON", `{"messages":` + diagnosticNumber + `}`, "thread payload: invalid response", thread},
		{"channel ID", `{"results":"### Result 1\nName: ` + diagnosticCanary + `"}`, "channel result missing ID", channels},
		{"user ID", `{"results":"### Result 1\nName: ` + diagnosticCanary + `"}`, "user result missing ID", users},
		{"channel header", `{"messages":"` + diagnosticCanary + `"}`, "channel header: invalid response", messages},
		{"message header", `{"messages":"Channel: test (C123)\n\n=== Message from ` + diagnosticCanary + ` === \nMessage TS: 1.000001\nbody"}`, "message time: invalid response", messages},
		{"thread header", `{"messages":"` + diagnosticCanary + `"}`, "missing replies separator", thread},
		{"reference JSON", `{"channels":[{"id":` + diagnosticNumber + `}]}`, "invalid Slack API response", reference},
		{"reference error", `{"ok":false,"error":"` + diagnosticCanary + `"}`, "Slack API reported an error", reference},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.parse(tc.raw)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, fmt.Sprintf("%+v", err), diagnosticCanary)
			require.NotContains(t, err.Error(), diagnosticNumber)
		})
	}
}

func TestPaginationDiagnosticsExcludeCursor(t *testing.T) {
	calls := 0
	err := walkPages(3, func(cursor string) (string, error) {
		if calls > 0 {
			require.Equal(t, diagnosticCanary, cursor)
		}
		calls++
		return diagnosticCanary, nil
	})
	require.EqualError(t, err, "MCP pagination repeated cursor")
	require.Equal(t, 2, calls)
}

func TestPersistenceDiagnosticsPreserveUnrelatedErrors(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, errors.New("database unavailable")} {
		require.True(t, err == persistenceError(err))
	}
	collision := fmt.Errorf("write batch: %w", &store.WorkspaceCollisionError{Entity: "message", ID: diagnosticCanary})
	err := persistenceError(collision)
	require.ErrorContains(t, err, "workspace identity conflict")
	require.NotContains(t, err.Error(), diagnosticCanary)
}

func TestSyncDiagnosticsExcludeReturnedConversationIdentity(t *testing.T) {
	for _, failThread := range []bool{false, true} {
		t.Run(fmt.Sprintf("thread=%v", failThread), func(t *testing.T) {
			t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string `json:"method"`
					Params struct {
						Name string `json:"name"`
					} `json:"params"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				switch request.Method {
				case "initialize":
					writeRPCResult(t, w, map[string]any{"protocolVersion": "2025-03-26"})
				case "notifications/initialized":
					w.WriteHeader(http.StatusAccepted)
				case "tools/list":
					writeRPCResult(t, w, map[string]any{"tools": []map[string]any{
						{"name": "slack_list_channels"}, {"name": "slack_get_users"},
						{"name": "slack_get_channel_history"}, {"name": "slack_get_thread_replies"},
					}})
				case "tools/call":
					switch request.Params.Name {
					case "slack_list_channels":
						writeToolText(t, w, map[string]any{"ok": true, "channels": []map[string]any{{"id": diagnosticCanary, "name": "test"}}})
					case "slack_get_users":
						writeToolText(t, w, map[string]any{"ok": true, "members": []any{}})
					case "slack_get_channel_history":
						if failThread {
							writeToolText(t, w, map[string]any{"ok": true, "messages": []map[string]any{{"ts": diagnosticNumber, "text": "root", "reply_count": 1}}})
						} else {
							writeToolText(t, w, map[string]any{"error": diagnosticCanary})
						}
					case "slack_get_thread_replies":
						writeToolText(t, w, map[string]any{"error": diagnosticCanary})
					default:
						t.Fatalf("unexpected tool %q", request.Params.Name)
					}
				default:
					t.Fatalf("unexpected method %q", request.Method)
				}
			}))
			defer server.Close()
			st, err := store.Open(filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			cfg := testMCPConfig(server.URL)
			cfg.ConnectorID = ""
			_, err = Sync(context.Background(), st, Options{WorkspaceID: "T123", Config: cfg, Full: true})
			if failThread {
				require.ErrorContains(t, err, "read MCP thread:")
			} else {
				require.ErrorContains(t, err, "read MCP channel:")
			}
			require.NotContains(t, fmt.Sprintf("%+v", err), diagnosticCanary)
			require.NotContains(t, err.Error(), diagnosticNumber)
			state, err := st.GetSyncState(context.Background(), SourceName, "workspace", "T123")
			require.ErrorIs(t, err, sql.ErrNoRows)
			require.Empty(t, state)
		})
	}
}
