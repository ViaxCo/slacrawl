package slackmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const admissionCanary = "SYNTHETIC_MCP_EXCLUDED_CONTENT"

func admissionGateway(t *testing.T, native bool, call func(string, map[string]any) map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		switch request.Method {
		case "initialize":
			writeRPCResult(t, w, map[string]any{"protocolVersion": "2025-03-26"})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			names := []string{"slack_search_channels", "slack_search_users", "slack_read_channel", "slack_read_thread"}
			if native {
				names = []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history", "slack_get_thread_replies"}
			}
			tools := []map[string]any{}
			for _, name := range names {
				tools = append(tools, map[string]any{"name": name})
			}
			writeRPCResult(t, w, map[string]any{"tools": tools})
		case "tools/call":
			writeToolText(t, w, call(request.Params.Name, request.Params.Arguments))
		default:
			t.Errorf("unexpected method %q", request.Method)
		}
	}))
}

func admissionStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func admissionTableSnapshot(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	result := map[string][]map[string]any{}
	for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_events", "message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state"} {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		result[table] = rows
	}
	return result
}

func TestMCPStrictTextAdmissionPrecedesData(t *testing.T) {
	for _, selectors := range [][]string{nil, {"named"}, {"CEXPLICIT"}} {
		t.Run(fmt.Sprint(selectors), func(t *testing.T) {
			var calls atomic.Int32
			server := admissionGateway(t, false, func(string, map[string]any) map[string]any { calls.Add(1); return map[string]any{} })
			defer server.Close()
			t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
			cfg := testMCPConfig(server.URL)
			cfg.ConnectorID = ""
			st := admissionStore(t)
			_, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Channels: selectors, Config: cfg, DMPolicy: admission.Exclude})
			require.ErrorContains(t, err, "requires native conversation type evidence")
			require.Zero(t, calls.Load())
			for table, rows := range admissionTableSnapshot(t, st) {
				require.Empty(t, rows, table)
			}
		})
	}
}

func TestMCPNativeCatalogAdmission(t *testing.T) {
	public := map[string]any{"id": "CONE", "name": "one", "is_channel": true}
	for _, tc := range []struct {
		name                string
		channels            []map[string]any
		selectors, excluded []string
		fail                bool
		failReason          string
		omitted             int
		admitted            []string
	}{
		{name: "public", admitted: []string{"CONE"}, channels: []map[string]any{public}},
		{name: "private", admitted: []string{"CONE"}, channels: []map[string]any{{"id": "CONE", "is_channel": true, "is_private": true}}},
		{name: "legacy", admitted: []string{"CONE"}, channels: []map[string]any{{"id": "CONE", "is_group": true, "is_private": true}}},
		{name: "misleading DM prefix", admitted: []string{"DONE"}, channels: []map[string]any{{"id": "DONE", "is_channel": true}}},
		{name: "IM veto", channels: []map[string]any{{"id": "CONE", "is_im": true, "is_channel": true}}, omitted: 1},
		{name: "MPIM veto", channels: []map[string]any{{"id": "CONE", "is_mpim": true, "is_channel": true}}, omitted: 1},
		{name: "private alone", channels: []map[string]any{{"id": "CONE", "is_private": true}}, fail: true},
		{name: "unknown", channels: []map[string]any{{"id": "CONE"}}, fail: true},
		{name: "conflicting flags", channels: []map[string]any{{"id": "CONE", "is_channel": true, "is_group": true}}, fail: true},
		{name: "duplicate DM", channels: []map[string]any{public, {"id": "CONE", "is_im": true}}, omitted: 1},
		{name: "duplicate unknown", channels: []map[string]any{public, {"id": "CONE"}}, fail: true},
		{name: "duplicate different channel kind", channels: []map[string]any{public, {"id": "CONE", "is_channel": true, "is_private": true}}, fail: true},
		{name: "duplicate same kind", admitted: []string{"CONE"}, channels: []map[string]any{public, public}},
		{name: "excluded duplicate alias", channels: []map[string]any{public, {"id": "CONE", "name": "excluded", "is_im": true}}, selectors: []string{"one"}, excluded: []string{"excluded"}},
		{name: "unselected unknown", admitted: []string{"CONE"}, channels: []map[string]any{public, {"id": "COTHER"}}, selectors: []string{"CONE"}},
		{name: "excluded unknown", admitted: []string{"CONE"}, channels: []map[string]any{public, {"id": "COTHER"}}, excluded: []string{"COTHER"}},
		{name: "direct ID cannot match a name", channels: []map[string]any{{"id": "COTHER", "name": "CONE", "is_channel": true}}, selectors: []string{"CONE"}, fail: true},
		{name: "direct ID exact case", channels: []map[string]any{{"id": "cone", "is_channel": true}}, selectors: []string{"CONE"}, fail: true},
		{name: "duplicate bad latest", channels: []map[string]any{public, {"id": "CONE", "is_channel": true, "latest": map[string]any{"channel": "CFOREIGN"}}}, fail: true},
		{name: "direct ID requires catalog", channels: []map[string]any{public}, selectors: []string{"CMISSING"}, fail: true},
		{name: "missing ID", channels: []map[string]any{{"is_channel": true}}, fail: true},
		{name: "foreign context", channels: []map[string]any{{"id": "CONE", "is_channel": true, "context_team_id": "TFOREIGN"}}, fail: true},
		{name: "same workspace DM", channels: []map[string]any{{"id": "CONE", "is_im": true, "is_channel": true, "context_team_id": "TLOCAL"}}, omitted: 1},
		{name: "foreign context DM", channels: []map[string]any{{"id": "CONE", "is_im": true, "context_team_id": "TFOREIGN"}}, fail: true, failReason: "conversation context workspace does not match"},
		{name: "foreign context duplicate DM", channels: []map[string]any{{"id": "CONE", "is_channel": true, "context_team_id": "TLOCAL"}, {"id": "CONE", "is_im": true, "context_team_id": "TFOREIGN"}}, fail: true, failReason: "conversation context workspace does not match"},
		{name: "bad latest", channels: []map[string]any{{"id": "CONE", "is_channel": true, "latest": map[string]any{"channel": "CFOREIGN", "text": admissionCanary}}}, fail: true},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", tc.name, reverse), func(t *testing.T) {
				channels := append([]map[string]any(nil), tc.channels...)
				if reverse {
					for a, b := 0, len(channels)-1; a < b; a, b = a+1, b-1 {
						channels[a], channels[b] = channels[b], channels[a]
					}
				}
				var catalogCalls, dataCalls atomic.Int32
				historyIDs := make(chan string, len(channels))
				server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
					if name == "slack_list_channels" {
						catalogCalls.Add(1)
						index := 0
						if args["cursor"] == "next" {
							index = 1
						}
						result := map[string]any{"ok": true, "channels": channels[index:]}
						if index == 0 && len(channels) > 1 {
							result["channels"] = channels[:1]
							result["response_metadata"] = map[string]any{"next_cursor": "next"}
						}
						return result
					}
					dataCalls.Add(1)
					if name == "slack_get_channel_history" {
						historyIDs <- args["channel_id"].(string)
					}
					return map[string]any{"ok": true}
				})
				defer server.Close()
				t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
				cfg := testMCPConfig(server.URL)
				cfg.ConnectorID = ""
				st := admissionStore(t)
				// latest-only must not conceal invalid catalog evidence before qualification.
				summary, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Channels: tc.selectors, ExcludeChannels: tc.excluded, Config: cfg, DMPolicy: admission.Exclude, LatestOnly: tc.fail, Full: !tc.fail})
				require.Equal(t, int32(min(len(channels), 2)), catalogCalls.Load())
				if tc.fail {
					require.Error(t, err)
					if tc.failReason != "" {
						require.ErrorContains(t, err, tc.failReason)
					}
					require.NotContains(t, err.Error(), admissionCanary)
					require.Zero(t, dataCalls.Load())
					for table, rows := range admissionTableSnapshot(t, st) {
						require.Empty(t, rows, table)
					}
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.omitted, summary.OmittedDM)
					require.Equal(t, len(tc.admitted), summary.Channels)
					require.Equal(t, len(tc.admitted) == 0, summary.NoEligibleConversations)
					var requested []string
					for len(historyIDs) > 0 {
						requested = append(requested, <-historyIDs)
					}
					require.ElementsMatch(t, tc.admitted, requested)
					rows, err := st.QueryReadOnly(context.Background(), "select id from channels")
					require.NoError(t, err)
					var expected []map[string]any
					for _, id := range tc.admitted {
						expected = append(expected, map[string]any{"id": id})
					}
					require.ElementsMatch(t, expected, rows)
				}
			})
		}
	}
}

func TestMCPNativeBlankIDPrecedesWrites(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, id := range []string{"", " \t"} {
			t.Run(fmt.Sprintf("policy=%v/id=%q", policy, id), func(t *testing.T) {
				var catalogCalls, dataCalls atomic.Int32
				server := admissionGateway(t, true, func(name string, _ map[string]any) map[string]any {
					if name == "slack_list_channels" {
						catalogCalls.Add(1)
						return map[string]any{"ok": true, "channels": []map[string]any{{"id": id, "is_channel": true}}}
					}
					dataCalls.Add(1)
					return map[string]any{"ok": true}
				})
				defer server.Close()
				t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
				cfg := testMCPConfig(server.URL)
				cfg.ConnectorID = ""
				st := admissionStore(t)
				_, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Full: true})
				require.EqualError(t, err, "MCP conversation is missing an ID")
				require.Equal(t, int32(1), catalogCalls.Load())
				require.Zero(t, dataCalls.Load())
				for table, rows := range admissionTableSnapshot(t, st) {
					require.Empty(t, rows, table)
				}
			})
		}
	}
}

func TestMCPSelectionKeepsDuplicateEvidence(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, mode := range []string{"all", "name", "id", "mixed", "repeated name"} {
			for _, tc := range []struct {
				name, conflict string
				excluded       []string
			}{
				{name: "valid aliases"},
				{name: "foreign context alias", conflict: "context workspace"},
				{name: "foreign latest alias", conflict: "message channel"},
				{name: "excluded alias precedes identity", conflict: "context workspace", excluded: []string{"#ALIAS"}},
				{name: "excluded ID", excluded: []string{"CONE"}},
			} {
				for _, reverse := range []bool{false, true} {
					t.Run(fmt.Sprintf("policy=%d/%s/%s/reverse=%v", policy, mode, tc.name, reverse), func(t *testing.T) {
						alias := map[string]any{"id": "CONE", "name": "alias", "is_channel": true, "context_team_id": "TLOCAL"}
						if tc.conflict == "context workspace" {
							alias["context_team_id"] = "TFOREIGN"
						} else if tc.conflict == "message channel" {
							alias["latest"] = map[string]any{"channel": "CFOREIGN", "text": admissionCanary}
						}
						catalog := []map[string]any{{"id": "CONE", "name": "one", "is_channel": true, "context_team_id": "TLOCAL"}, alias}
						if reverse {
							catalog[0], catalog[1] = catalog[1], catalog[0]
						}
						var selectors []string
						switch mode {
						case "name":
							selectors = []string{"one"}
						case "id":
							selectors = []string{"CONE"}
						case "mixed":
							selectors = []string{"CONE", "one"}
						case "repeated name":
							selectors = []string{"one", "one"}
						}
						var catalogCalls, dataCalls, historyCalls atomic.Int32
						server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
							if name == "slack_list_channels" {
								catalogCalls.Add(1)
								return map[string]any{"ok": true, "channels": catalog}
							}
							dataCalls.Add(1)
							if name == "slack_get_channel_history" {
								historyCalls.Add(1)
								require.Equal(t, "CONE", args["channel_id"])
							}
							return map[string]any{"ok": true}
						})
						defer server.Close()
						t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
						cfg := testMCPConfig(server.URL)
						cfg.ConnectorID = ""
						st := admissionStore(t)
						summary, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Channels: selectors, ExcludeChannels: tc.excluded, Full: true})
						wantCatalog := int32(1)
						if policy != admission.Exclude {
							if mode == "id" {
								wantCatalog = 0
							} else if mode == "repeated name" {
								wantCatalog = 2
							}
						}
						require.Equal(t, wantCatalog, catalogCalls.Load())
						excluded := tc.name == "excluded ID" || (len(tc.excluded) > 0 && wantCatalog > 0)
						if tc.conflict != "" && wantCatalog > 0 && !excluded {
							require.ErrorContains(t, err, tc.conflict)
							require.Zero(t, dataCalls.Load())
							for table, rows := range admissionTableSnapshot(t, st) {
								require.Empty(t, rows, table)
							}
							return
						}
						require.NoError(t, err)
						wantHistory := 1
						if excluded {
							wantHistory = 0
						} else if mode == "all" && policy != admission.Exclude {
							wantHistory = 2
						}
						require.Equal(t, int32(wantHistory), historyCalls.Load())
						require.Equal(t, wantHistory, summary.Channels)
						require.Equal(t, excluded && policy == admission.Exclude, summary.NoEligibleConversations)
						rows, err := st.QueryReadOnly(context.Background(), "select name, kind from channels")
						require.NoError(t, err)
						if excluded {
							require.Empty(t, rows)
						} else {
							require.Len(t, rows, 1)
							if policy != admission.Exclude && (mode == "mixed" || mode == "id") {
								require.Equal(t, "", rows[0]["name"], "first selected ID stub remains the payload")
								require.Equal(t, "mcp_channel", rows[0]["kind"])
							} else if policy != admission.Exclude && mode != "all" {
								require.Equal(t, "one", rows[0]["name"], "first matching payload remains selected")
							}
						}
					})
				}
			}
		}
	}
}

func TestMCPNamedSelectionKeepsEarlierEvidence(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, reverse := range []bool{false, true} {
			for _, selectLater := range []bool{false, true} {
				t.Run(fmt.Sprintf("policy=%d/reverse=%v/later=%v", policy, reverse, selectLater), func(t *testing.T) {
					var catalogCalls, dataCalls atomic.Int32
					historyIDs := make(chan string, 2)
					server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
						if name != "slack_list_channels" {
							dataCalls.Add(1)
							if name == "slack_get_channel_history" {
								historyIDs <- args["channel_id"].(string)
							}
							return map[string]any{"ok": true}
						}
						catalog := []map[string]any{{"id": "CFIRST", "name": "first", "is_channel": true}, {"id": "CSECOND", "name": "second", "is_channel": true, "context_team_id": "TLOCAL"}}
						if catalogCalls.Add(1) == 1 {
							catalog[1]["context_team_id"] = "TFOREIGN"
						}
						if reverse {
							catalog[0], catalog[1] = catalog[1], catalog[0]
						}
						return map[string]any{"ok": true, "channels": catalog}
					})
					defer server.Close()
					t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
					cfg := testMCPConfig(server.URL)
					cfg.ConnectorID = ""
					st := admissionStore(t)
					selectors := []string{"first"}
					if selectLater {
						selectors = append(selectors, "second")
					}
					summary, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Channels: selectors, Full: true})
					wantCatalog := int32(len(selectors))
					if policy == admission.Exclude {
						wantCatalog = 1
					}
					require.Equal(t, wantCatalog, catalogCalls.Load())
					if selectLater {
						require.ErrorContains(t, err, "context workspace")
						require.Zero(t, dataCalls.Load())
						for table, rows := range admissionTableSnapshot(t, st) {
							require.Empty(t, rows, table)
						}
					} else {
						require.NoError(t, err)
						require.Equal(t, 1, summary.Channels)
						require.Equal(t, int32(1), dataCalls.Load())
						require.Len(t, historyIDs, 1)
						require.Equal(t, "CFIRST", <-historyIDs)
						rows, queryErr := st.QueryReadOnly(context.Background(), "select id from channels")
						require.NoError(t, queryErr)
						require.Equal(t, []map[string]any{{"id": "CFIRST"}}, rows)
					}
				})
			}
		}
	}
}

func TestMCPNativeExcludedArchiveRemainsUnchanged(t *testing.T) {
	t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
	var catalogCalls, dataCalls atomic.Int32
	server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
		if name == "slack_list_channels" {
			catalogCalls.Add(1)
			return map[string]any{"ok": true, "channels": []map[string]any{{"id": "CONE", "name": "one", "is_mpim": true, "topic": map[string]any{"value": admissionCanary}}}}
		}
		dataCalls.Add(1)
		return map[string]any{"ok": true}
	})
	defer server.Close()
	cfg := testMCPConfig(server.URL)
	cfg.ConnectorID = ""
	st := admissionStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "CONE", WorkspaceID: "TLOCAL", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "CONE", WorkspaceID: "TLOCAL", TS: "1710000000.000001", Text: "preserved", NormalizedText: "preserved", RawJSON: "{}", SourceName: "api-user", SourceRank: 1, UpdatedAt: now}, []store.Mention{{Type: "user", TargetID: "UKEEP"}}))
	require.NoError(t, st.SetSyncState(ctx, "mcp", "workspace", "TLOCAL", "old-checkpoint"))
	before := admissionTableSnapshot(t, st)
	for range 2 {
		summary, err := Sync(ctx, st, Options{WorkspaceID: "TLOCAL", Channels: []string{"CONE"}, Config: cfg, DMPolicy: admission.Exclude, Full: true})
		require.NoError(t, err)
		require.Equal(t, 1, summary.OmittedDM)
		require.Zero(t, summary.Channels)
		require.True(t, summary.NoEligibleConversations)
		for table, rows := range admissionTableSnapshot(t, st) {
			require.ElementsMatch(t, before[table], rows, table)
			require.NotContains(t, fmt.Sprint(rows), admissionCanary)
		}
	}
	require.Equal(t, int32(2), catalogCalls.Load())
	require.Zero(t, dataCalls.Load())
}

func TestMCPStrictNativeCatalogRequiresSuccess(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, okValue := range []any{nil, false} {
			for _, later := range []bool{false, true} {
				for _, empty := range []bool{false, true} {
					t.Run(fmt.Sprintf("policy=%v/ok=%v/later=%v/empty=%v", policy, okValue, later, empty), func(t *testing.T) {
						server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
							result := map[string]any{"ok": true}
							if name == "slack_list_channels" {
								if !empty {
									result["channels"] = []map[string]any{{"id": "CONE", "is_channel": true}}
								}
								if later && args["cursor"] != "next" {
									result["response_metadata"] = map[string]any{"next_cursor": "next"}
								} else {
									delete(result, "ok")
									if okValue != nil {
										result["ok"] = okValue
									}
								}
							}
							return result
						})
						defer server.Close()
						t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
						cfg := testMCPConfig(server.URL)
						cfg.ConnectorID = ""
						st := admissionStore(t)
						_, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, LatestOnly: true})
						if policy == admission.Exclude {
							require.ErrorContains(t, err, "did not report successful Slack response")
							for table, rows := range admissionTableSnapshot(t, st) {
								require.Empty(t, rows, table)
							}
						} else {
							require.NoError(t, err)
						}
					})
				}
			}
		}
	}
}

func TestMCPNoEligibleOwnerFact(t *testing.T) {
	for _, mode := range []string{"empty", "excluded", "latest-only"} {
		t.Run(mode, func(t *testing.T) {
			server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
				result := map[string]any{"ok": true}
				if name == "slack_list_channels" && mode != "empty" {
					result["channels"] = []map[string]any{{"id": "CONE", "is_channel": true}}
				}
				return result
			})
			defer server.Close()
			t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
			cfg := testMCPConfig(server.URL)
			cfg.ConnectorID = ""
			opts := Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: admission.Exclude, LatestOnly: true}
			if mode == "excluded" {
				opts.ExcludeChannels = []string{"CONE"}
			}
			st := admissionStore(t)
			summary, err := Sync(context.Background(), st, opts)
			require.NoError(t, err)
			require.Zero(t, summary.Channels)
			require.Zero(t, summary.OmittedDM)
			require.Equal(t, mode != "latest-only", summary.NoEligibleConversations)
			if summary.NoEligibleConversations {
				for table, rows := range admissionTableSnapshot(t, st) {
					require.Empty(t, rows, table)
				}
			}
		})
	}
}

func TestMCPNativeDefaultPayloadsAndReplay(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			server := admissionGateway(t, true, func(name string, args map[string]any) map[string]any {
				result := map[string]any{"ok": true}
				switch name {
				case "slack_list_channels":
					result["channels"] = []map[string]any{{"id": "CONE", "name": "one", "is_im": true, "context_team_id": "TLOCAL"}}
				case "slack_get_channel_history":
					result["messages"] = []map[string]any{{"channel": "CONE", "context_team_id": "TLOCAL", "ts": "1710000000.000001", "user": "UEXTERNAL", "team": "TEXTERNAL", "text": "allowed"}}
				}
				return result
			})
			defer server.Close()
			t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
			cfg := testMCPConfig(server.URL)
			cfg.ConnectorID = ""
			st := admissionStore(t)
			opts := Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Full: true}
			for range 2 {
				_, err := Sync(context.Background(), st, opts)
				require.NoError(t, err)
				rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
				require.NoError(t, err)
				require.Equal(t, `{"id":"CONE","name":"one","kind":"public_channel","topic":"","purpose":"","permalink":"","is_private":false,"is_archived":false}`, rows[0]["raw_json"])
				rows, err = st.QueryReadOnly(context.Background(), "select raw_json from messages")
				require.NoError(t, err)
				require.Equal(t, `{"channel_id":"CONE","channel_name":"","ts":"1710000000.000001","thread_ts":"","author_id":"UEXTERNAL","author_name":"","occurred_at":"","text":"allowed","reply_count":0,"latest_reply":""}`, rows[0]["raw_json"])
				for _, table := range []string{"message_events", "message_event_heads"} {
					rows, err = st.QueryReadOnly(context.Background(), "select * from "+table)
					require.NoError(t, err)
					require.Len(t, rows, 1, table)
				}
			}
		})
	}
}

func TestMCPMessageAdmissionPrecedesAffectedWrites(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, mode := range []string{"history-channel", "history-old", "history-empty-ts", "history-blank-ts", "history-context", "history-message", "history-previous_message", "history-root", "thread-channel", "thread-context", "thread-ts", "thread-empty-ts", "thread-blank-ts", "thread-message", "thread-first-parent", "text-channel-page", "text-history-empty-ts", "text-history-blank-ts", "text-thread-page", "text-thread-parent-ts"} {
			if policy == admission.Exclude && strings.HasPrefix(mode, "text-") {
				continue
			} // Strict text stops at discovery instead.
			t.Run(fmt.Sprintf("policy=%v/%s", policy, mode), func(t *testing.T) {
				native := !strings.HasPrefix(mode, "text-")
				thread := strings.HasPrefix(mode, "thread-") || strings.HasPrefix(mode, "text-thread-")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				repliesStarted := make(chan struct{})
				releaseReplies := make(chan struct{})
				var firstReplies atomic.Bool
				server := admissionGateway(t, native, func(name string, args map[string]any) map[string]any {
					if thread && (name == "slack_get_thread_replies" || name == "slack_read_thread") && firstReplies.CompareAndSwap(false, true) {
						close(repliesStarted)
						select {
						case <-releaseReplies:
						case <-ctx.Done():
						}
					}
					result := map[string]any{"ok": true}
					switch name {
					case "slack_list_channels":
						result["channels"] = []map[string]any{{"id": "CONE", "is_channel": true}}
					case "slack_search_channels":
						result["results"] = "### Result 1\nChannel ID: CONE\nName: one"
					case "slack_search_users":
						result["results"] = ""
					case "slack_get_channel_history":
						root := map[string]any{"channel": "CONE", "ts": "1710000000.000001", "text": "history root"}
						if thread {
							root["reply_count"] = 1
							result["messages"] = []map[string]any{root}
							return result
						}
						bad := map[string]any{"channel": "CONE", "ts": "1710000001.000002", "text": admissionCanary}
						switch mode {
						case "history-channel":
							bad["channel"] = "CFOREIGN"
						case "history-old":
							bad["channel"] = "CFOREIGN"
							bad["ts"] = "1.000001"
						case "history-empty-ts":
							bad["ts"] = ""
						case "history-blank-ts":
							bad["ts"] = " \t"
						case "history-context":
							bad["context_team_id"] = "TFOREIGN"
						default:
							bad[strings.TrimPrefix(mode, "history-")] = map[string]any{"channel": "CFOREIGN"}
						}
						result["messages"] = []map[string]any{root, bad}
					case "slack_get_thread_replies":
						root := map[string]any{"channel": "CONE", "ts": "1710000000.000001", "text": admissionCanary, "reply_count": 99}
						bad := map[string]any{"channel": "CONE", "ts": "1710000002.000003", "thread_ts": "1710000000.000001", "text": admissionCanary}
						switch mode {
						case "thread-channel":
							bad["channel"] = "CFOREIGN"
						case "thread-context":
							bad["context_team_id"] = "TFOREIGN"
						case "thread-ts":
							bad["thread_ts"] = "1710000009.000009"
						case "thread-empty-ts":
							bad["ts"] = ""
						case "thread-blank-ts":
							bad["ts"] = " \t"
						case "thread-message":
							bad["message"] = map[string]any{"channel": "CFOREIGN"}
						case "thread-first-parent":
							result["messages"] = []map[string]any{{"channel": "CONE", "ts": "1710000009.000009", "text": admissionCanary}, root}
							return result
						}
						result["messages"] = []map[string]any{root, {"ts": "1710000001.000002", "thread_ts": "1710000000.000001", "text": admissionCanary}, bad}
					case "slack_read_channel":
						channel := "CONE"
						text := "history root"
						if mode == "text-channel-page" {
							if args["cursor"] == "next" {
								channel = "CFOREIGN"
								text = admissionCanary
							} else {
								result["pagination_info"] = "cursor `next`"
							}
						}
						result["messages"] = "Channel: one (" + channel + ")\n\n=== Message from External (UEXTERNAL) at 2024-03-09T16:00:00Z === \nMessage TS: 1710000000.000001\n" + text
						if thread {
							result["messages"] = result["messages"].(string) + "\nThread: 1 replies (latest: 1710000001.000002)"
						}
						if mode == "text-history-empty-ts" || mode == "text-history-blank-ts" {
							badTS := ""
							if mode == "text-history-blank-ts" {
								badTS = " \t"
							}
							result["messages"] = result["messages"].(string) + "\n\n=== Message from External (UEXTERNAL) at 2024-03-09T16:00:01Z === \nMessage TS: " + badTS + "\n" + admissionCanary
						}
					case "slack_read_thread":
						ts := "1710000000.000001"
						if mode == "text-thread-page" {
							if args["cursor"] == "next" {
								ts = "1710000009.000009"
							} else {
								result["pagination_info"] = "cursor `next`"
							}
						}
						replyTS := "1710000001.000002"
						if mode == "text-thread-parent-ts" {
							replyTS = ts
						}
						result["messages"] = "From: External (UEXTERNAL)\nTime: 2024-03-09T16:00:00Z\nMessage TS: " + ts + "\n" + admissionCanary + "\n\n=== THREAD REPLIES\n\n--- Reply 1 ---\nFrom: External (UEXTERNAL)\nTime: 2024-03-09T16:00:01Z\nMessage TS: " + replyTS + "\n" + admissionCanary
					}
					return result
				})
				defer server.Close()
				t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
				cfg := testMCPConfig(server.URL)
				cfg.ConnectorID = ""
				st := admissionStore(t)
				opts := Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Full: true}
				if mode == "history-old" || mode == "history-empty-ts" {
					opts.Since = "100.000000"
				}
				assertHistory := func(row map[string]any, complete bool) {
					t.Helper()
					adapter := "codex"
					if native {
						adapter = "reference"
					}
					key, keyErr := json.Marshal([]string{"TLOCAL", "CONE", adapter, normalizeTimestamp(opts.Since)})
					require.NoError(t, keyErr)
					value, ok := row["value"].(string)
					require.True(t, ok)
					var state store.MCPHistoryState
					require.NoError(t, json.Unmarshal([]byte(value), &state))
					require.NotEmpty(t, state.Revision)
					expected := store.MCPHistoryState{Complete: complete, Revision: state.Revision}
					if complete {
						expected.Latest = "1710000000.000001"
					} else {
						expected.Pending = new(normalizeTimestamp(opts.Since))
					}
					require.Equal(t, expected, state)
					updated, ok := row["updated_at"].(string)
					require.True(t, ok)
					_, timeErr := time.Parse(time.RFC3339Nano, updated)
					require.NoError(t, timeErr)
					require.Equal(t, map[string]any{"source_name": SourceName, "entity_type": store.MCPHistoryEntityType, "entity_id": string(key), "value": value, "updated_at": updated}, row)
				}
				var err error
				var admitted map[string][]map[string]any
				if thread {
					finished := make(chan struct{})
					var syncErr error
					go func() {
						_, syncErr = Sync(ctx, st, opts)
						close(finished)
					}()
					defer func() { cancel(); <-finished }()
					select {
					case <-repliesStarted:
					case <-finished:
						t.Fatalf("sync returned before the replies request: %v", syncErr)
					}
					// Valid history owns durable work before replies admission; a
					// rejected payload must preserve that entire committed snapshot.
					admitted = admissionTableSnapshot(t, st)
					require.Len(t, admitted["sync_state"], 2)
					var pending map[string]any
					historyCount := 0
					for _, row := range admitted["sync_state"] {
						if row["entity_type"] == store.MCPHistoryEntityType {
							assertHistory(row, true)
							historyCount++
						} else {
							pending = row
						}
					}
					require.Equal(t, 1, historyCount)
					require.NotNil(t, pending)
					generation, ok := pending["value"].(string)
					require.True(t, ok)
					require.NotEmpty(t, generation)
					updatedAt, ok := pending["updated_at"].(string)
					require.True(t, ok)
					_, timeErr := time.Parse(time.RFC3339Nano, updatedAt)
					require.NoError(t, timeErr)
					require.Equal(t, map[string]any{
						"source_name": SourceName, "entity_type": store.ThreadPendingEntityType,
						"entity_id": `["TLOCAL","CONE","1710000000.000001"]`, "value": generation, "updated_at": updatedAt,
					}, pending)
					current, currentErr := st.ThreadWorkCurrent(ctx, store.ThreadWork{
						SourceName: SourceName, WorkspaceID: "TLOCAL", ChannelID: "CONE", TS: "1710000000.000001", Generation: generation,
					})
					require.NoError(t, currentErr)
					require.True(t, current)
					close(releaseReplies)
					<-finished
					err = syncErr
				} else {
					_, err = Sync(ctx, st, opts)
				}
				require.Error(t, err)
				if strings.Contains(mode, "empty-ts") || strings.Contains(mode, "blank-ts") {
					require.ErrorContains(t, err, "message timestamp is empty")
				} else if mode == "text-thread-parent-ts" {
					require.ErrorContains(t, err, "reply timestamp")
				}
				require.NotContains(t, err.Error(), admissionCanary)
				require.NotContains(t, err.Error(), "CFOREIGN")
				require.NotContains(t, err.Error(), "TFOREIGN")
				after := admissionTableSnapshot(t, st)
				for _, table := range []string{"messages", "message_files", "message_mentions", "message_fts", "message_events", "message_event_heads", "embedding_jobs", "sync_state"} {
					rows := after[table]
					require.NotContains(t, fmt.Sprint(rows), admissionCanary, table)
					if !thread {
						if table == "sync_state" {
							require.Len(t, rows, 1)
							assertHistory(rows[0], false)
						} else {
							require.Empty(t, rows, table)
						}
					}
				}
				status, err := st.Status(ctx)
				require.NoError(t, err)
				require.True(t, status.LastSyncAt.IsZero())
				if thread {
					require.Equal(t, admitted, after, "rejected replies must not change admitted state")
					rows, err := st.QueryReadOnly(context.Background(), "select text,reply_count from messages")
					require.NoError(t, err)
					require.Equal(t, []map[string]any{{"text": "history root", "reply_count": int64(1)}}, rows)
				}
			})
		}
	}
}

func TestMCPStdioAdmission(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%v", native), func(t *testing.T) {
			mode := "text"
			if native {
				mode = "native"
			}
			t.Setenv("SLACRAWL_MCP_ADMISSION_HELPER", mode)
			cfg := testMCPConfig("")
			cfg.ConnectorID = ""
			cfg.Transport = "stdio"
			cfg.Command = os.Args[0]
			cfg.Args = []string{"-test.run=^TestMCPAdmissionStdioHelper$"}
			cfg.EnvAllowlist = []string{"SLACRAWL_MCP_ADMISSION_HELPER"}
			st := admissionStore(t)
			summary, err := Sync(context.Background(), st, Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: admission.Exclude, Full: true})
			if native {
				require.NoError(t, err)
				require.Equal(t, 1, summary.Channels)
				require.Equal(t, 1, summary.Messages)
				require.Equal(t, 1, summary.OmittedDM)
			} else {
				require.ErrorContains(t, err, "requires native conversation type evidence")
			}
			for table, rows := range admissionTableSnapshot(t, st) {
				require.NotContains(t, fmt.Sprint(rows), admissionCanary, table)
				if !native {
					require.Empty(t, rows, table)
				}
			}
		})
	}
}

func TestMCPAdmissionStdioHelper(t *testing.T) {
	mode := os.Getenv("SLACRAWL_MCP_ADMISSION_HELPER")
	if mode == "" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var req struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				os.Exit(0)
			}
			os.Exit(2)
		}
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26"}
		case "notifications/initialized":
			continue
		case "tools/list":
			names := []string{"slack_search_channels", "slack_search_users", "slack_read_channel"}
			if mode == "native" {
				names = []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history"}
			}
			tools := []map[string]any{}
			for _, name := range names {
				tools = append(tools, map[string]any{"name": name})
			}
			result = map[string]any{"tools": tools}
		case "tools/call":
			if mode != "native" {
				os.Exit(3)
			}
			payload := map[string]any{"ok": true}
			switch req.Params.Name {
			case "slack_list_channels":
				payload["channels"] = []map[string]any{{"id": "CKEEP", "is_channel": true}, {"id": "CDM", "is_im": true, "name": admissionCanary}}
			case "slack_get_users":
				payload["members"] = []any{}
			case "slack_get_channel_history":
				if req.Params.Arguments["channel_id"] != "CKEEP" {
					os.Exit(4)
				}
				payload["messages"] = []map[string]any{{"channel": "CKEEP", "ts": "1710000000.000001", "text": "safe"}}
			default:
				os.Exit(5)
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				os.Exit(6)
			}
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
		default:
			os.Exit(7)
		}
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
			os.Exit(8)
		}
	}
}
