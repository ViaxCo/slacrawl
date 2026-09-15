package slackmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/share"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const nativeEvidenceObject = `{
  "id":"CONE", "name":"one", "context_team_id":"TLOCAL",
  "is_channel":true,"is_private":false,"is_group":false,"is_im":false,"is_mpim":false,
  "topic":{"value":"topic"},"purpose":{"value":"purpose"},
  "unmodeled":{"value":"SYNTHETIC_NATIVE_RAW_CANARY","number":1.00}
}`

type channelEvidenceCall struct {
	name string
	args map[string]any
}

func nativeEvidenceCatalog(objects string) string {
	return `{"ok":true,"envelope_canary":"SYNTHETIC_CATALOG_ONLY","channels":[` + objects + `]}`
}

// Put the literal catalog inside the tool text without re-encoding its objects.
func nativeEvidenceGateway(t *testing.T, catalog string) (*httptest.Server, chan channelEvidenceCall) {
	t.Helper()
	calls := make(chan channelEvidenceCall, 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Name string         `json:"name"`
				Args map[string]any `json:"arguments"`
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
			calls <- channelEvidenceCall{request.Params.Name, request.Params.Args}
			var text string
			switch request.Params.Name {
			case "slack_list_channels":
				text = catalog
			case "slack_get_users":
				text = `{"ok":true,"members":[]}`
			case "slack_get_channel_history":
				require.Equal(t, "CONE", request.Params.Args["channel_id"])
				text = `{"ok":true,"messages":[{"channel":"CONE","ts":"1710000000.000001","user":"UONE","text":"selected message"}]}`
			default:
				t.Errorf("unexpected evidence tool %q", request.Params.Name)
				text = `{"ok":false}`
			}
			writeRPCResult(t, w, map[string]any{"content": []map[string]any{{"type": "text", "text": text}}})
		default:
			t.Errorf("unexpected evidence method %q", request.Method)
		}
	}))
	t.Cleanup(server.Close)
	return server, calls
}

func nativeEvidenceStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st, path
}

func nativeEvidenceOptions(t *testing.T, server *httptest.Server, policy admission.DMPolicy) Options {
	t.Helper()
	t.Setenv("TEST_MCP_TOKEN", "synthetic-token")
	cfg := testMCPConfig(server.URL)
	cfg.ConnectorID = ""
	return Options{WorkspaceID: "TLOCAL", Config: cfg, DMPolicy: policy, Full: true}
}

func requireNativeEvidenceCalls(t *testing.T, calls chan channelEvidenceCall, names ...string) {
	t.Helper()
	var got []channelEvidenceCall
	for len(calls) > 0 {
		got = append(got, <-calls)
	}
	var want []channelEvidenceCall
	for _, name := range names {
		args := map[string]any{"limit": float64(100)}
		if name == "slack_get_channel_history" {
			args["channel_id"] = "CONE"
		}
		want = append(want, channelEvidenceCall{name, args})
	}
	require.Equal(t, want, got)
}

func requireNativeEvidenceSelection(t *testing.T, path string, qualified bool) {
	t.Helper()
	ctx := context.Background()
	plan, err := share.PrepareExportSelection(ctx, path, share.ExportSelection{
		WorkspaceID: "TLOCAL", Channels: []share.ExportChannelSelection{{ChannelID: "CONE"}},
		Messages: []share.ExportMessageSelection{{ChannelID: "CONE", TS: "1710000000.000001", Text: share.ExportTextChoice{Mode: share.ExportTextKeep}}},
	})
	if !qualified {
		require.ErrorContains(t, err, "lacks unambiguous current public-channel evidence")
		require.Zero(t, plan)
		require.NotContains(t, err.Error(), "CANARY")
		return
	}
	require.NoError(t, err)
	projection, err := share.ResolveExportSelection(ctx, path, plan)
	require.NoError(t, err)
	require.Equal(t, share.ExportProjection{
		WorkspaceID: "TLOCAL", WorkspaceLabel: "TLOCAL",
		Channels: []share.ExportChannelSelection{{ChannelID: "CONE", Label: "CONE"}},
		Messages: []share.ExportMessage{{ChannelID: "CONE", TS: "1710000000.000001", UserID: new("UONE"), Text: "selected message", ThreadTS: new(""), EditedTS: new("")}},
	}, projection)
	body, err := json.Marshal(projection)
	require.NoError(t, err)
	require.NotContains(t, string(body), "CANARY")
}

func TestMCPNativeChannelObjectOwnsBytes(t *testing.T) {
	input := []byte(nativeEvidenceObject)
	var channel referenceChannel
	require.NoError(t, json.Unmarshal(input, &channel))
	require.Equal(t, nativeEvidenceObject, string(channel.raw))
	require.Equal(t, admission.PublicChannel, channel.NativeFlags.Kind())
	for i := range input {
		input[i] = 'x'
	}
	require.Equal(t, nativeEvidenceObject, string(channel.raw))
	first := channel
	require.NoError(t, json.Unmarshal([]byte(`{"id":"COTHER"}`), &channel))
	require.Equal(t, nativeEvidenceObject, string(first.raw))
	require.Equal(t, `{"id":"COTHER"}`, string(channel.raw))
}

func TestMCPNativeChannelEvidenceFromSync(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(nativeEvidenceObject))
			st, path := nativeEvidenceStore(t)
			summary, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, policy))
			require.NoError(t, err)
			require.Equal(t, 1, summary.Channels)
			require.Equal(t, 1, summary.Messages)
			requireNativeEvidenceCalls(t, calls, "slack_list_channels", "slack_get_users", "slack_get_channel_history")
			rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"raw_json": nativeEvidenceObject}}, rows)
			before := admissionTableSnapshot(t, st)
			requireNativeEvidenceSelection(t, path, true)
			require.Equal(t, before, admissionTableSnapshot(t, st))
		})
	}
}

func TestMCPNativeChannelUnqualifiedEvidenceSurvives(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"missing-private", strings.Replace(nativeEvidenceObject, `"is_private":false,`, "", 1)},
		{"null-private", strings.Replace(nativeEvidenceObject, `"is_private":false`, `"is_private":null`, 1)},
		{"missing-channel", strings.Replace(nativeEvidenceObject, `"is_channel":true,`, "", 1)},
		{"null-channel", strings.Replace(nativeEvidenceObject, `"is_channel":true`, `"is_channel":null`, 1)},
		{"no-flags", `{"id":"CONE","name":"one"}`},
		{"duplicate-private", strings.Replace(nativeEvidenceObject, `"is_private":false`, `"is_private":true,"is_private":false`, 1)},
		{"escaped-duplicate", strings.Replace(nativeEvidenceObject, `"is_private":false`, `"is_\u0070rivate":true,"is_private":false`, 1)},
		{"casefold-duplicate", strings.Replace(nativeEvidenceObject, `"is_private":false`, `"IS_PRIVATE":true,"is_private":false`, 1)},
		{"duplicate-id", strings.Replace(nativeEvidenceObject, `"id":"CONE"`, `"id":"CFOREIGN","id":"CONE"`, 1)},
		{"alternate-channel", strings.Replace(nativeEvidenceObject, `"id":"CONE"`, `"id":"CONE","channel_id":"CFOREIGN"`, 1)},
		{"alternate-workspace", strings.Replace(nativeEvidenceObject, `"id":"CONE"`, `"id":"CONE","workspace_id":"TFOREIGN"`, 1)},
		{"alternate-team", strings.Replace(nativeEvidenceObject, `"id":"CONE"`, `"id":"CONE","team_id":"TFOREIGN"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(tc.raw))
			st, path := nativeEvidenceStore(t)
			_, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, admission.Default))
			require.NoError(t, err)
			requireNativeEvidenceCalls(t, calls, "slack_list_channels", "slack_get_users", "slack_get_channel_history")
			rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"raw_json": tc.raw}}, rows)
			requireNativeEvidenceSelection(t, path, false)
		})
	}
}

func TestMCPNativeChannelEvidenceContextPrecedesWrites(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(strings.Replace(nativeEvidenceObject, "TLOCAL", "TFOREIGN", 1)))
			st, _ := nativeEvidenceStore(t)
			_, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, policy))
			require.ErrorContains(t, err, "conversation context workspace does not match")
			require.NotContains(t, err.Error(), "CANARY")
			requireNativeEvidenceCalls(t, calls, "slack_list_channels")
			for table, rows := range admissionTableSnapshot(t, st) {
				require.Empty(t, rows, table)
			}
		})
	}
}

func TestMCPNativeChannelEvidenceExcludesOtherObjects(t *testing.T) {
	catalog := nativeEvidenceObject + `,{"id":"DDM","is_im":true,"latest":{"text":"SYNTHETIC_DM_LATEST_CANARY"},"private":"SYNTHETIC_DM_RAW_CANARY"},{"id":"GMPIM","is_mpim":true,"latest":{"text":"SYNTHETIC_MPIM_LATEST_CANARY"},"private":"SYNTHETIC_MPIM_RAW_CANARY"}`
	server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(catalog))
	st, path := nativeEvidenceStore(t)
	summary, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, admission.Exclude))
	require.NoError(t, err)
	require.Equal(t, 2, summary.OmittedDM)
	require.Equal(t, 1, summary.Channels)
	requireNativeEvidenceCalls(t, calls, "slack_list_channels", "slack_get_users", "slack_get_channel_history")
	rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"raw_json": nativeEvidenceObject}}, rows)
	for table, rows := range admissionTableSnapshot(t, st) {
		body, err := json.Marshal(rows)
		require.NoError(t, err)
		for _, excluded := range []string{"DDM", "GMPIM", "SYNTHETIC_DM_", "SYNTHETIC_MPIM_", "SYNTHETIC_CATALOG_ONLY"} {
			require.NotContains(t, string(body), excluded, table)
		}
	}
	requireNativeEvidenceSelection(t, path, true)
}

func TestMCPNativeChannelEvidenceDuplicateSelection(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%v", policy, reverse), func(t *testing.T) {
				first, second := nativeEvidenceObject, strings.Replace(nativeEvidenceObject, `"name":"one"`, `"name":"alias"`, 1)
				if reverse {
					first, second = second, first
				}
				server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(first+","+second))
				st, _ := nativeEvidenceStore(t)
				summary, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, policy))
				require.NoError(t, err)
				names := []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history"}
				if policy != admission.Exclude {
					names = append(names, "slack_get_channel_history")
				}
				require.Equal(t, len(names)-2, summary.Channels)
				requireNativeEvidenceCalls(t, calls, names...)
				rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
				require.NoError(t, err)
				require.Equal(t, []map[string]any{{"raw_json": first}}, rows)
			})
		}
	}
}

func TestMCPNativeChannelRepeatedCatalogKeepsAdmission(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			// encoding/json reuses the channel slice element for the second key.
			// Its typed IM veto survives; raw evidence remains only the final object.
			last := `{"id":"CONE","is_channel":true}`
			catalog := `{"ok":true,"channels":[{"id":"CONE","is_im":true}],"channels":[` + last + `]}`
			server, calls := nativeEvidenceGateway(t, catalog)
			st, path := nativeEvidenceStore(t)
			summary, err := Sync(context.Background(), st, nativeEvidenceOptions(t, server, policy))
			require.NoError(t, err)
			if policy == admission.Exclude {
				require.Equal(t, 1, summary.OmittedDM)
				require.True(t, summary.NoEligibleConversations)
				requireNativeEvidenceCalls(t, calls, "slack_list_channels")
				for table, rows := range admissionTableSnapshot(t, st) {
					require.Empty(t, rows, table)
				}
				return
			}
			requireNativeEvidenceCalls(t, calls, "slack_list_channels", "slack_get_users", "slack_get_channel_history")
			rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"raw_json": last}}, rows)
			requireNativeEvidenceSelection(t, path, false)
		})
	}
}

func TestMCPNativeChannelEvidencePreservesExistingRows(t *testing.T) {
	for _, kind := range []string{"legacy", "richer"} {
		t.Run(kind, func(t *testing.T) {
			server, calls := nativeEvidenceGateway(t, nativeEvidenceCatalog(nativeEvidenceObject))
			st, path := nativeEvidenceStore(t)
			ctx, now := context.Background(), time.Unix(1710000000, 0).UTC()
			require.NoError(t, st.EnsureWorkspace(ctx, store.Workspace{ID: "TLOCAL", RawJSON: "{}", UpdatedAt: now}))
			channel := store.Channel{ID: "CONE", WorkspaceID: "TLOCAL", Name: "old-name", Kind: "public_channel", RawJSON: `{"id":"CONE","kind":"public_channel","is_private":false}`, UpdatedAt: now}
			if kind == "richer" {
				channel.Name, channel.Topic, channel.RawJSON = "richer-name", "richer-topic", strings.Replace(nativeEvidenceObject, `"name":"one"`, `"name":"richer-name"`, 1)
			}
			require.NoError(t, st.UpsertChannel(ctx, channel))
			before, err := st.QueryReadOnly(ctx, "select * from channels")
			require.NoError(t, err)
			for range 2 {
				_, err := Sync(ctx, st, nativeEvidenceOptions(t, server, admission.Exclude))
				require.NoError(t, err)
				requireNativeEvidenceCalls(t, calls, "slack_list_channels", "slack_get_users", "slack_get_channel_history")
				after, err := st.QueryReadOnly(ctx, "select * from channels")
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
			requireNativeEvidenceSelection(t, path, kind == "richer")
		})
	}
}

func TestMCPChannelEvidenceLegacyPayloads(t *testing.T) {
	for _, mode := range []string{"native-id", "text-id", "text-catalog"} {
		for _, policy := range []admission.DMPolicy{admission.Default, admission.Include} {
			t.Run(fmt.Sprintf("%s/%d", mode, policy), func(t *testing.T) {
				calls := make(chan channelEvidenceCall, 8)
				server := admissionGateway(t, mode == "native-id", func(name string, args map[string]any) map[string]any {
					calls <- channelEvidenceCall{name, args}
					switch name {
					case "slack_get_channel_history":
						return map[string]any{"ok": true, "messages": []any{}}
					case "slack_search_channels":
						return map[string]any{"results": "### Result 1\nChannel ID: CONE\nName: #one\nType: public_channel"}
					case "slack_search_users":
						return map[string]any{"results": ""}
					case "slack_read_channel":
						return map[string]any{"messages": "Channel: one (CONE)"}
					default:
						t.Errorf("unexpected legacy tool %q", name)
						return map[string]any{}
					}
				})
				defer server.Close()
				st, _ := nativeEvidenceStore(t)
				opts := nativeEvidenceOptions(t, server, policy)
				if mode != "text-catalog" {
					opts.Channels = []string{"CONE"}
				}
				_, err := Sync(context.Background(), st, opts)
				require.NoError(t, err)
				rows, err := st.QueryReadOnly(context.Background(), "select raw_json from channels")
				require.NoError(t, err)
				want := `{"id":"CONE","name":"","kind":"mcp_channel","topic":"","purpose":"","permalink":"","is_private":false,"is_archived":false}`
				if mode == "native-id" {
					requireNativeEvidenceCalls(t, calls, "slack_get_channel_history")
				} else {
					want = `{"id":"CONE","name":"one","kind":"mcp_channel","topic":"","purpose":"","permalink":"","is_private":false,"is_archived":false}`
					var names []string
					for len(calls) > 0 {
						call := <-calls
						names = append(names, call.name)
						if call.name == "slack_read_channel" {
							require.Equal(t, map[string]any{"channel_id": "CONE", "limit": float64(100), "response_format": "detailed"}, call.args)
						}
					}
					if mode == "text-catalog" {
						want = strings.Replace(want, `"kind":"mcp_channel"`, `"kind":"public_channel"`, 1)
						require.Equal(t, []string{"slack_search_channels", "slack_search_users", "slack_read_channel"}, names)
					} else {
						require.Equal(t, []string{"slack_read_channel"}, names)
					}
				}
				require.Equal(t, []map[string]any{{"raw_json": want}}, rows)
			})
		}
	}
}
