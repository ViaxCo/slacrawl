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

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSlackExportDMPolicyFromCLIConfig(t *testing.T) {
	for _, format := range []string{"zip", "directory"} {
		for _, policy := range []string{"omitted", "false", "true"} {
			for _, existing := range []bool{false, true} {
				for _, dry := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/existing=%t/dry=%t", format, policy, existing, dry), func(t *testing.T) {
						ctx := context.Background()
						cfg, configPath := importAdmissionConfig(t, policy)
						if existing {
							seedImportArchive(t, cfg.DBPath)
						}
						before := importAdmissionSnapshot(t, cfg.DBPath)
						configBefore, err := os.ReadFile(configPath)
						require.NoError(t, err)
						export := importAdmissionExport(t, format, importAdmissionFiles())
						var output bytes.Buffer
						app := &App{Stdout: &output, Stderr: &output}
						args := []string{"--config", configPath, "--json", "import", export, "--workspace", "TEXPORT"}
						if dry {
							args = append(args, "--dry-run")
						}
						runErr := app.Run(ctx, args)
						after := importAdmissionSnapshot(t, cfg.DBPath)
						configAfter, err := os.ReadFile(configPath)
						require.NoError(t, err)
						var report struct {
							Messages, DMs, MPIMs, Channels int
							DryRun                         bool `json:"dry_run"`
						}
						decodeErr := json.Unmarshal(output.Bytes(), &report)
						dmRows := importAdmissionCount(after["messages"], "CDIRECT") + importAdmissionCount(after["messages"], "CMULTI")
						pathsCreated := importAdmissionExists(t, cfg.CacheDir) || importAdmissionExists(t, cfg.LogDir) || (!existing && importAdmissionExists(t, cfg.DBPath))
						wantMessages, wantDM := 4, 1
						if policy == "false" {
							wantMessages, wantDM = 2, 0
						}
						safeOutput := output.String()
						if runErr != nil {
							safeOutput += runErr.Error()
						}
						leaked := strings.Contains(safeOutput, "export-dm-canary") || strings.Contains(safeOutput, "export-mpim-canary")
						t.Logf("import observation: error=%t messages=%d dms=%d mpims=%d dm_rows=%d paths_created=%t dry=%t existing=%t", runErr != nil, report.Messages, report.DMs, report.MPIMs, dmRows, pathsCreated, dry, existing)
						boundaryOK := runErr == nil && decodeErr == nil && report.Messages == wantMessages && report.DMs == wantDM && report.MPIMs == wantDM && !leaked
						if dry {
							boundaryOK = boundaryOK && !pathsCreated
						}
						if !boundaryOK {
							t.Fatalf("import boundary: error=%t messages=%d dms=%d mpims=%d dm_rows=%d paths_created=%t output_leak=%t", runErr != nil, report.Messages, report.DMs, report.MPIMs, dmRows, pathsCreated, leaked)
						}
						require.Equal(t, 2, report.Channels)
						require.Equal(t, dry, report.DryRun)
						require.Equal(t, configBefore, configAfter)
						if dry {
							require.Equal(t, before, after)
							return
						}
						require.Equal(t, wantDM*2, dmRows)
						serialized, err := json.Marshal(after)
						require.NoError(t, err)
						for _, canary := range []string{"export-public-canary", "export-private-canary"} {
							require.Contains(t, string(serialized), canary)
						}
						for _, canary := range []string{"export-dm-canary", "export-mpim-canary"} {
							if policy == "false" {
								require.NotContains(t, string(serialized), canary)
							} else {
								require.Contains(t, string(serialized), canary)
							}
						}
						for _, table := range []string{"messages", "message_files", "message_mentions", "message_events", "message_event_heads", "message_fts"} {
							require.NotEmpty(t, after[table], table)
						}
					})
				}
			}
		}
	}
}

func importAdmissionConfig(t *testing.T, policy string) (config.Config, string) {
	t.Helper()
	root := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "archive", "archive.db")
	cfg.CacheDir = filepath.Join(root, "runtime", "cache")
	cfg.LogDir = filepath.Join(root, "runtime", "logs")
	cfg.Share.Remote = ""
	cfg.Share.RepoPath = filepath.Join(root, "share")
	cfg.Slack.Bot.Enabled = false
	cfg.Slack.User.Enabled = false
	cfg.Slack.App.Enabled = false
	cfg.Slack.Desktop.Enabled = false
	cfg.Slack.Desktop.Path = filepath.Join(root, "desktop")
	cfg.Slack.MCP.AuthPath = filepath.Join(root, "auth.json")
	if policy != "omitted" {
		include := policy == "true"
		cfg.Sync.IncludeDMs = &include
	} else {
		cfg.Sync.IncludeDMs = nil
	}
	configPath := filepath.Join(root, "config.toml")
	require.NoError(t, cfg.Save(configPath))
	return cfg, configPath
}

func importAdmissionFiles() map[string]string {
	files := map[string]string{
		"users.json":    `[{"id":"UAUTHOR","name":"synthetic"}]`,
		"channels.json": `[{"id":"CPUBLIC","name":"public"}]`,
		"groups.json":   `[{"id":"CPRIVATE","name":"private"}]`,
		"dms.json":      `[{"id":"CDIRECT","name":"direct"}]`,
		"mpims.json":    `[{"id":"CMULTI","name":"multi"}]`,
	}
	for i, pair := range [][2]string{{"public", "export-public-canary"}, {"private", "export-private-canary"}, {"direct", "export-dm-canary"}, {"multi", "export-mpim-canary"}} {
		files[pair[0]+"/2026-01-01.json"] = "[" + importAdmissionMessage(fmt.Sprintf("%d.000001", i+1), pair[1]) + "]"
	}
	return files
}

func importAdmissionMessage(ts, canary string) string {
	raw := map[string]any{"type": "message", "ts": ts, "user": "UAUTHOR", "text": canary + " <@UMENTION|" + canary + ">",
		"files":  []any{map[string]any{"id": "F" + canary, "title": canary, "name": canary, "url_private": "https://example.com/" + canary}},
		"blocks": []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": canary}}},
	}
	blob, _ := json.Marshal(raw)
	return string(blob)
}

func importAdmissionExport(t *testing.T, format string, files map[string]string) string {
	t.Helper()
	if format == "zip" {
		return writeImportFixtureZip(t, files)
	}
	root := t.TempDir()
	for name, body := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(target), 0700))
		require.NoError(t, os.WriteFile(target, []byte(body), 0600))
	}
	return root
}

func seedImportArchive(t *testing.T, dbPath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(dbPath), 0700))
	st, err := store.Open(dbPath)
	require.NoError(t, err)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "TPRIOR", Name: "prior", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "CPRIOR", WorkspaceID: "TPRIOR", Name: "prior", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertMessage(ctx, store.Message{ChannelID: "CPRIOR", TS: "0.000001", WorkspaceID: "TPRIOR", Text: "prior", NormalizedText: "prior", SourceName: "api-user", SourceRank: 1, RawJSON: "{}", UpdatedAt: now}, nil))
	require.NoError(t, st.Close())
}

func importAdmissionExists(t *testing.T, name string) bool {
	t.Helper()
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func importAdmissionSnapshot(t *testing.T, dbPath string) map[string][]map[string]any {
	t.Helper()
	result := map[string][]map[string]any{}
	if !importAdmissionExists(t, dbPath) {
		return result
	}
	st, err := store.OpenReadOnly(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	for _, table := range []string{"workspaces", "channels", "users", "messages", "message_files", "message_mentions", "message_events", "message_event_heads", "message_fts", "sync_state", "embedding_jobs"} {
		order := "rowid"
		if table == "message_event_heads" {
			order = "channel_id,ts,event_type,source_name"
		}
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table+" order by "+order)
		require.NoError(t, err, table)
		result[table] = rows
	}
	return result
}

func importAdmissionCount(rows []map[string]any, channel string) int {
	count := 0
	for _, row := range rows {
		if row["channel_id"] == channel {
			count++
		}
	}
	return count
}
