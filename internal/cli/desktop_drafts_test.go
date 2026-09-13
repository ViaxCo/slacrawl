package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestDesktopDraftPolicyAcrossCLIPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setting    string
		args       []string
		wantDrafts int
		wantSent   int
	}{
		{"default", "", []string{"sync", "--source", "desktop"}, 2, 0},
		{"enabled", "true", []string{"sync", "--source", "desktop"}, 2, 0},
		{"desktop", "false", []string{"sync", "--source", "desktop"}, 0, 0},
		{"wiretap", "false", []string{"sync", "--source", "wiretap"}, 0, 0},
		{"watch", "false", []string{"watch", "--desktop-every", "1h"}, 0, 0},
		{"all workspaces", "false", []string{"sync", "--source", "all"}, 0, 2},
		{"explicit hybrid", "false", []string{"sync", "--source", "hybrid", "--workspace", "T1"}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, configPath := desktopDraftConfig(t, tc.setting)
			server := newCLIFanoutSlackServer(t, map[string]string{"xoxb-draft-one": "T1", "xoxb-draft-two": "T2"})
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var stdout, stderr bytes.Buffer
			app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: server.URL + "/", httpClient: server.Client()}
			if tc.args[0] == "watch" {
				app.Stdout = &cancelAfterWrite{Writer: &stdout, cancel: cancel}
			}
			err := app.Run(ctx, append([]string{"--config", configPath, "--json"}, tc.args...))
			if tc.args[0] == "watch" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}

			st, err := store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			rows, err := st.QueryReadOnly(context.Background(), "select count(*) as count from messages where subtype = 'desktop_draft'")
			require.NoError(t, err)
			require.Equal(t, int64(tc.wantDrafts), rows[0]["count"])
			rows, err = st.QueryReadOnly(context.Background(), "select count(*) as count from messages where source_name = 'api-bot'")
			require.NoError(t, err)
			require.Equal(t, int64(tc.wantSent), rows[0]["count"])
			status, err := st.Status(context.Background())
			require.NoError(t, err)
			wantWorkspaces := 2
			if tc.name == "explicit hybrid" {
				wantWorkspaces = 1
			}
			require.Equal(t, wantWorkspaces, status.Workspaces)
			if tc.wantDrafts == 0 {
				for _, table := range draftArchiveTables {
					rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
					require.NoError(t, err)
					payload, err := json.Marshal(rows)
					require.NoError(t, err)
					for _, forbidden := range []string{"draft-body-canary", "CDRAFTONLY", "DDRAFTONLY"} {
						require.NotContains(t, string(payload), forbidden, table)
						require.NotContains(t, stdout.String()+stderr.String(), forbidden)
					}
				}
				count, err := st.GetSyncState(context.Background(), "desktop", "local_storage", "draft_count")
				require.NoError(t, err)
				require.Equal(t, "0", count)
			}
		})
	}
}

func TestExcludedDesktopDraftLeavesLegacyArchiveUnchanged(t *testing.T) {
	cfg, configPath := desktopDraftConfig(t, "false")
	ctx := context.Background()
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, st.UpsertMessage(ctx, store.Message{
		WorkspaceID: "T1", ChannelID: "CDRAFTONLY", TS: "draft:171:T1:draft-1",
		Subtype: "desktop_draft", Text: "previously archived", NormalizedText: "previously archived",
		SourceName: "desktop-draft", SourceRank: 3, RawJSON: `{"previous":true}`, UpdatedAt: time.Now().UTC(),
	}, nil))
	before := map[string][]map[string]any{}
	for _, table := range draftArchiveTables[3:] {
		before[table], err = st.QueryReadOnly(ctx, "select * from "+table)
		require.NoError(t, err)
	}
	require.NoError(t, st.Close())
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output}
	require.NoError(t, app.Run(ctx, []string{"--config", configPath, "--json", "sync", "--source", "desktop"}))
	st, err = store.OpenReadOnly(cfg.DBPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	for table, expected := range before {
		if table == "sync_state" {
			continue
		}
		rows, err := st.QueryReadOnly(ctx, "select * from "+table)
		require.NoError(t, err)
		require.Equal(t, expected, rows, table)
	}
	rows, err := st.QueryReadOnly(ctx, "select * from channels where id = 'CDRAFTONLY'")
	require.NoError(t, err)
	require.Empty(t, rows)
	require.NotContains(t, output.String(), "draft-body-canary")
}

var draftArchiveTables = []string{
	"workspaces", "channels", "users", "messages", "message_files", "message_events",
	"message_event_heads", "message_mentions", "message_fts", "embedding_jobs", "sync_state",
}

func desktopDraftConfig(t *testing.T, setting string) (config.Config, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "Slack")
	db, err := leveldb.OpenFile(filepath.Join(root, "Local Storage", "leveldb"), nil)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("_https://app.slack.comlocalConfig_v2"), []byte(`{"teams":{"T1":{"id":"T1","name":"One","user_id":"U1"},"T2":{"id":"T2","name":"Two","user_id":"U2"}}}`), nil))
	for i, channelID := range []string{"CDRAFTONLY", "DDRAFTONLY"} {
		key := fmt.Sprintf("_https://app.slack.compersist-v1::T%d::U%d::drafts", i+1, i+1)
		body := fmt.Sprintf(`{"unifiedDrafts":{"draft-%d":{"destinations":[{"channel_id":%q,"thread_ts":"1710000000.000100"}],"ops":[{"insert":"draft-body-canary <@UDRAFTONLY>"}],"last_updated_ts":1710000000}}}`, i+1, channelID)
		require.NoError(t, db.Put([]byte(key), []byte(body), nil))
	}
	require.NoError(t, db.Close())
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Slack.Desktop.Path = root
	cfg.Slack.App.Enabled = false
	cfg.Slack.User.Enabled = false
	cfg.Workspaces = []config.Workspace{
		{ID: "T1", BotTokenEnv: "SLACRAWL_DRAFT_TEST_ONE"},
		{ID: "T2", BotTokenEnv: "SLACRAWL_DRAFT_TEST_TWO"},
	}
	t.Setenv("SLACRAWL_DRAFT_TEST_ONE", "xoxb-draft-one")
	t.Setenv("SLACRAWL_DRAFT_TEST_TWO", "xoxb-draft-two")
	configPath := filepath.Join(dir, "config.toml")
	require.NoError(t, cfg.Save(configPath))
	if setting != "" {
		body, err := os.ReadFile(configPath)
		require.NoError(t, err)
		updated := strings.Replace(string(body), "[slack.desktop]\n", "[slack.desktop]\ninclude_drafts = "+setting+"\n", 1)
		require.NotEqual(t, string(body), updated)
		require.NoError(t, os.WriteFile(configPath, []byte(updated), 0o600))
	}
	// Exercise both config loading and saving: an explicit false must survive.
	cfg, err = config.Load(configPath)
	require.NoError(t, err)
	require.NoError(t, cfg.Save(configPath))
	return cfg, configPath
}
