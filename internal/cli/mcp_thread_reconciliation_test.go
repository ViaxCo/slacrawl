package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/share"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestMCPSlicedReturnedRootEvidenceFromCLI(t *testing.T) {
	for _, source := range []string{"mcp", "connector"} {
		for _, mode := range []string{"root-child", "child-root", "root-page", "child-page", "retained-child", "child-only", "priority-child"} {
			t.Run(source+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture := &mcpReconcileFixture{}
				cfg, path := mcpReconcileConfig(t, fixture)
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Unix(1700000000, 0).UTC()
				mcpReconcileCatalog(t, st, now)
				old := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: mcpWorkRoot, Text: "old retained root", NormalizedText: "old retained root", ReplyCount: 1, SourceName: "mcp", SourceRank: 4, RawJSON: "{}", UpdatedAt: now}
				require.NoError(t, st.UpsertMessage(ctx, old, nil))
				_, err = st.PrepareThreadWork(ctx, "mcp", "TLOCAL", "C123")
				require.NoError(t, err)
				beforePending := mcpWorkPending(t, st)
				beforeRoot := apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkRoot+"'")
				fixture.mu.Lock()
				root := mcpWorkMessage(mcpWorkNewRoot, "", false)
				child := mcpWorkMessage(mcpWorkNewChild, mcpWorkNewRoot, false)
				fixture.messages = []map[string]any{root, child}
				switch mode {
				case "child-root":
					fixture.messages = []map[string]any{child, root}
				case "root-page", "child-page":
					first, last := root, child
					if mode == "child-page" {
						first, last = child, root
					}
					fixture.messages = []map[string]any{first}
					for i := 0; i < 499; i++ {
						fixture.messages = append(fixture.messages, mcpWorkMessage(fmt.Sprintf("171000%04d.000000", 2000+i), "", false))
					}
					fixture.messages = append(fixture.messages, last)
				case "retained-child", "priority-child":
					retained := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: mcpWorkNewChild, ThreadTS: mcpWorkNewRoot, Text: "retained child", NormalizedText: "retained child", SourceName: "mcp", SourceRank: 4, RawJSON: "{}", UpdatedAt: now}
					if mode == "priority-child" {
						retained.ThreadTS, retained.SourceName, retained.SourceRank = "", "api-user", 1
					}
					require.NoError(t, st.UpsertMessage(ctx, retained, nil))
					if mode == "retained-child" {
						fixture.messages = fixture.messages[:1]
					}
				case "child-only":
					child["thread_ts"] = mcpWorkRoot
					fixture.messages = []map[string]any{child}
				}
				fixture.mu.Unlock()
				require.NoError(t, st.Close())
				beforeConfig, err := os.ReadFile(path)
				require.NoError(t, err)
				args := []string{"--config", path, "--json", "sync", "--source", source, "--workspace", "TLOCAL", "--channels", "C123", "--with-media=false", "--since", mcpWorkSince}
				if mode == "child-root" || mode == "child-page" || mode == "priority-child" {
					args = append(args, "--full")
				}
				var stdout, stderr bytes.Buffer
				err = (&App{Stdout: &stdout, Stderr: &stderr}).Run(ctx, args)
				require.NoError(t, err)
				require.NotEmpty(t, stdout.String())
				calls, serverErrors := fixture.observed()
				require.Empty(t, serverErrors)
				want := []mcpCoverageCall{{"slack_list_channels", map[string]any{"limit": float64(20)}}, {"slack_get_channel_history", map[string]any{"channel_id": "C123", "limit": float64(501)}}}
				if mode != "child-only" && mode != "priority-child" {
					want = append(want, mcpCoverageCall{"slack_get_thread_replies", map[string]any{"channel_id": "C123", "thread_ts": mcpWorkNewRoot}})
				}
				require.Equal(t, want, calls)
				st, err = store.OpenReadOnly(cfg.DBPath)
				require.NoError(t, err)
				defer func() { require.NoError(t, st.Close()) }()
				require.Equal(t, beforePending, mcpWorkPending(t, st), "Since and FullSince leave every ordinary queue field unchanged")
				require.Equal(t, beforeRoot, apiKeyRows(t, st, "select * from messages where ts='"+mcpWorkRoot+"'"))
				replies := apiKeyRows(t, st, "select coalesce(thread_ts,'') as thread_ts,source_name,source_rank from messages where ts='1710001003.000000'")
				if len(want) == 3 {
					require.Equal(t, []map[string]any{{"thread_ts": mcpWorkNewRoot, "source_name": "mcp", "source_rank": int64(4)}}, replies)
				} else {
					require.Empty(t, replies)
				}
				require.Equal(t, apiKeyRows(t, st, "select channel_id || '|' || ts as message_key from messages order by message_key"), apiKeyRows(t, st, "select message_key from message_fts order by message_key"))
				afterConfig, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, beforeConfig, afterConfig)
			})
		}
	}
}

func TestMCPNoToolReconcilesMergedTombstonesFromCLI(t *testing.T) {
	for _, source := range []string{"mcp", "connector"} {
		for _, mode := range []string{"deleted_ts", "subtype", "live", "fresh", "history-error", "since", "full-since"} {
			t.Run(source+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture := &mcpReconcileFixture{noTool: true, historyError: mode == "history-error"}
				cfg, path := mcpReconcileConfig(t, fixture)
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Unix(1700000000, 0).UTC()
				mcpReconcileCatalog(t, st, now)
				root := store.Message{WorkspaceID: "TLOCAL", ChannelID: "C123", TS: mcpWorkRoot, Text: "root", NormalizedText: "root", ReplyCount: 1, SourceName: "mcp", SourceRank: 4, RawJSON: "{}", UpdatedAt: now}
				require.NoError(t, st.UpsertMessage(ctx, root, nil))
				require.NoError(t, st.SetSyncState(ctx, "mcp", "workspace", "TLOCAL", "2020-01-01T00:00:00Z"))
				if mode != "fresh" {
					_, err := st.PrepareThreadWork(ctx, "mcp", "TLOCAL", "C123")
					require.NoError(t, err)
				}
				beforePending := mcpWorkPending(t, st)
				beforeWorkspace := mcpWorkWorkspace(t, st)
				if mode != "live" && mode != "fresh" {
					donor, err := store.Open(filepath.Join(t.TempDir(), "donor.db"))
					require.NoError(t, err)
					mcpReconcileCatalog(t, donor, now)
					deleted := root
					deleted.ReplyCount, deleted.UpdatedAt = 0, now.Add(time.Hour)
					if mode == "subtype" {
						deleted.Subtype = "message_deleted"
					} else {
						deleted.DeletedTS = "1710000009.000000"
					}
					require.NoError(t, donor.UpsertMessage(ctx, deleted, nil))
					opts := share.Options{RepoPath: filepath.Join(t.TempDir(), "share")}
					_, err = share.Export(ctx, donor, opts)
					require.NoError(t, err)
					_, err = share.Import(ctx, st, opts)
					require.NoError(t, err)
					require.NoError(t, donor.Close())
					require.Equal(t, beforePending, mcpWorkPending(t, st), "share merge leaves local progress for its lifecycle owner")
					require.Equal(t, []map[string]any{{"deleted_ts": deleted.DeletedTS, "subtype": deleted.Subtype, "reply_count": int64(0)}}, apiKeyRows(t, st, "select coalesce(deleted_ts,'') as deleted_ts,coalesce(subtype,'') as subtype,reply_count from messages"))
				}
				messagesBefore := apiKeyRows(t, st, "select * from messages")
				require.NoError(t, st.Close())
				beforeConfig, err := os.ReadFile(path)
				require.NoError(t, err)
				args := []string{"--config", path, "--json", "sync", "--source", source, "--workspace", "TLOCAL", "--channels", "C123", "--with-media=false"}
				if mode == "since" || mode == "full-since" {
					args = append(args, "--since", mcpWorkSince)
				}
				if mode == "full-since" {
					args = append(args, "--full")
				}
				for attempt := 0; attempt < 2; attempt++ {
					fixture.resetCalls()
					var stdout, stderr bytes.Buffer
					runErr := (&App{Stdout: &stdout, Stderr: &stderr}).Run(ctx, args)
					failed := mode == "live" || mode == "history-error"
					if mode == "live" {
						require.EqualError(t, runErr, mcpWorkToolError)
					} else if mode == "history-error" {
						require.ErrorContains(t, runErr, "Slack API reported an error")
					} else {
						require.NoError(t, runErr)
					}
					require.Equal(t, !failed, stdout.Len() != 0)
					calls, serverErrors := fixture.observed()
					require.Empty(t, serverErrors)
					require.Equal(t, []mcpCoverageCall{{"slack_list_channels", map[string]any{"limit": float64(20)}}, {"slack_get_channel_history", map[string]any{"channel_id": "C123", "limit": float64(501)}}}, calls)
					st, err = store.OpenReadOnly(cfg.DBPath)
					require.NoError(t, err)
					pending := mcpWorkPending(t, st)
					if mode == "deleted_ts" || mode == "subtype" || mode == "fresh" {
						require.Empty(t, pending)
					} else {
						require.Equal(t, beforePending, pending)
					}
					if failed {
						require.Equal(t, beforeWorkspace, mcpWorkWorkspace(t, st))
					} else {
						require.NotEqual(t, beforeWorkspace, mcpWorkWorkspace(t, st))
					}
					require.Equal(t, messagesBefore, apiKeyRows(t, st, "select * from messages"))
					require.NoError(t, st.Close())
				}
				afterConfig, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, beforeConfig, afterConfig)
			})
		}
	}
}

func mcpReconcileCatalog(t *testing.T, st *store.Store, now time.Time) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TLOCAL", Name: "fixture", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "C123", WorkspaceID: "TLOCAL", Name: "fixture", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
}

func mcpReconcileConfig(t *testing.T, fixture *mcpReconcileFixture) (config.Config, string) {
	t.Helper()
	cfg, path := userPrimaryConfig(t)
	cfg.Slack.Bot.Enabled, cfg.Slack.User.Enabled, cfg.Slack.App.Enabled = false, false, false
	cfg.Sync.IncludeDMs = new(false)
	server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(server.Close)
	cfg.Slack.MCP.Enabled, cfg.Slack.MCP.Transport, cfg.Slack.MCP.BaseURL = true, "http", server.URL
	cfg.Slack.MCP.TokenEnv, cfg.Slack.MCP.AccountIDEnv = "SLACRAWL_RECONCILE_TOKEN", "SLACRAWL_RECONCILE_ACCOUNT"
	cfg.Slack.MCP.ConnectorID = ""
	cfg.Slack.MCP.PageSize, cfg.Slack.MCP.SearchLimit, cfg.Slack.MCP.MaxPages = 501, 20, 1
	t.Setenv(cfg.Slack.MCP.TokenEnv, "synthetic-reconcile-token")
	t.Setenv(cfg.Slack.MCP.AccountIDEnv, "")
	require.NoError(t, cfg.Save(path))
	return cfg, path
}

type mcpReconcileFixture struct {
	mu                   sync.Mutex
	messages             []map[string]any
	noTool, historyError bool
	calls                []mcpCoverageCall
	errors               []string
}

func (f *mcpReconcileFixture) observed() ([]mcpCoverageCall, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mcpCoverageCall{}, f.calls...), append([]string{}, f.errors...)
}

func (f *mcpReconcileFixture) resetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *mcpReconcileFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage
		Method string
		Params struct {
			Name      string
			Arguments map[string]any
		}
	}
	fail := func(reason string) {
		f.mu.Lock()
		f.errors = append(f.errors, reason)
		f.mu.Unlock()
		http.Error(w, "synthetic fixture rejected request", http.StatusBadRequest)
	}
	if r.Header.Get("Authorization") != "Bearer synthetic-reconcile-token" {
		fail("unexpected credential")
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail("invalid JSON")
		return
	}
	var result any
	switch req.Method {
	case "initialize":
		result = map[string]any{"protocolVersion": "2025-03-26"}
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
		return
	case "tools/list":
		names := []string{"slack_list_channels", "slack_get_users", "slack_get_channel_history"}
		if !f.noTool {
			names = append(names, "slack_get_thread_replies")
		}
		tools := []map[string]any{}
		for _, name := range names {
			tools = append(tools, map[string]any{"name": name})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		f.mu.Lock()
		f.calls = append(f.calls, mcpCoverageCall{req.Params.Name, req.Params.Arguments})
		f.mu.Unlock()
		payload := map[string]any{"ok": true}
		switch req.Params.Name {
		case "slack_list_channels":
			payload["channels"] = []map[string]any{{"id": "C123", "name": "fixture", "is_channel": true}}
		case "slack_get_channel_history":
			if req.Params.Arguments["limit"] != float64(501) {
				fail("history page size")
				return
			}
			f.mu.Lock()
			messages := f.messages
			f.mu.Unlock()
			if messages == nil {
				messages = []map[string]any{}
			}
			payload["messages"] = messages
			if f.historyError {
				payload["ok"], payload["error"] = false, "synthetic_history_error"
			}
		case "slack_get_thread_replies":
			root, _ := req.Params.Arguments["thread_ts"].(string)
			if root != mcpWorkRoot && root != mcpWorkNewRoot {
				fail("unexpected thread")
				return
			}
			payload["messages"] = []map[string]any{mcpWorkMessage(root, "", false), mcpWorkMessage("1710001003.000000", root, false)}
		default:
			fail("unexpected tool")
			return
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			fail("marshal response")
			return
		}
		result = map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}
	default:
		fail("unexpected method")
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
		f.mu.Lock()
		f.errors = append(f.errors, "response write")
		f.mu.Unlock()
	}
}
