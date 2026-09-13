package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestDesktopDMPolicyAcrossCLIPaths(t *testing.T) {
	for _, route := range []struct {
		name       string
		args       []string
		workspaces int
	}{
		{"desktop", []string{"sync", "--source", "desktop"}, 2},
		{"wiretap", []string{"sync", "--source", "wiretap"}, 2},
		{"watch", []string{"watch", "--desktop-every", "1h"}, 2},
		{"all", []string{"sync", "--source", "all"}, 2},
		{"hybrid", []string{"sync", "--source", "hybrid", "--workspace", "T1"}, 1},
	} {
		for _, setting := range []string{"", "false", "true"} {
			for _, drafts := range []string{"", "false", "true"} {
				t.Run(fmt.Sprintf("%s/dms=%s/drafts=%s", route.name, setting, drafts), func(t *testing.T) {
					cfg, path := desktopDMConfig(t, setting, drafts)
					server := newCLIFanoutSlackServer(t, map[string]string{"xoxb-draft-one": "T1", "xoxb-draft-two": "T2"})
					defer server.Close()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var stdout, stderr bytes.Buffer
					app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: server.URL + "/", httpClient: server.Client()}
					if route.name == "watch" {
						app.Stdout = &cancelAfterWrite{Writer: &stdout, cancel: cancel}
					}
					err := app.Run(ctx, append([]string{"--config", path, "--json"}, route.args...))
					if route.name == "watch" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NoError(t, err)
					}
					st, err := store.OpenReadOnly(cfg.DBPath)
					require.NoError(t, err)
					defer func() { require.NoError(t, st.Close()) }()
					rows, err := st.QueryReadOnly(context.Background(), "select count(*) as n from messages where source_name = 'desktop-indexeddb'")
					require.NoError(t, err)
					want := route.workspaces * 2
					if setting == "false" {
						want = route.workspaces
					}
					require.Equal(t, int64(want), rows[0]["n"])
					wantDrafts := route.workspaces
					if setting == "false" {
						wantDrafts = 1
					}
					if drafts == "false" {
						wantDrafts = 0
					}
					rows, err = st.QueryReadOnly(context.Background(), "select count(*) as n from messages where subtype = 'desktop_draft'")
					require.NoError(t, err)
					require.Equal(t, int64(wantDrafts), rows[0]["n"])
					status, err := st.Status(context.Background())
					require.NoError(t, err)
					require.Equal(t, route.workspaces, status.Workspaces)
					if setting == "false" {
						for _, table := range draftArchiveTables {
							rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
							require.NoError(t, err)
							body, err := json.Marshal(rows)
							require.NoError(t, err)
							for _, canary := range []string{"desktop-dm-canary", "CDMONLY", "DDRAFTONLY"} {
								require.NotContains(t, string(body), canary, table)
								require.NotContains(t, stdout.String()+stderr.String(), canary)
							}
						}
						require.Contains(t, stdout.String(), `"admission"`)
					}
				})
			}
		}
	}
}

func TestDesktopAdmissionHumanOmissions(t *testing.T) {
	for _, decoder := range []string{"available", "unavailable", "partial"} {
		t.Run(decoder, func(t *testing.T) {
			cfg, path := desktopDMConfig(t, "false", "false")
			if decoder == "unavailable" {
				t.Setenv("PATH", t.TempDir())
			}
			if decoder == "partial" {
				require.NoError(t, os.WriteFile(filepath.Join(cfg.Slack.Desktop.Path, "IndexedDB", "https_app.slack.com_0.indexeddb.blob", "invalid"), []byte{0xff, 0x11, 0x02, 0xff}, 0o600))
			}
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			require.NoError(t, app.Run(context.Background(), []string{"--config", path, "sync", "--source", "desktop"}))
			require.Contains(t, output.String(), "Completed with omissions")
			require.NotContains(t, output.String(), "desktop-dm-canary")
			if decoder == "unavailable" {
				require.Contains(t, output.String(), "decoder unavailable")
			}
			if decoder == "partial" {
				require.Contains(t, output.String(), "decode failures")
			}
		})
	}
}

func desktopDMConfig(t *testing.T, setting, drafts string) (config.Config, string) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is required for desktop fixtures")
	}
	cfg, path := desktopDraftConfig(t, drafts)
	if setting != "" {
		cfg.Sync.IncludeDMs = new(setting == "true")
	}
	require.NoError(t, cfg.Save(path))
	for _, workspace := range []string{"T1", "T2"} {
		publicID, dmID := "CDRAFTONLY", "CDMONLY"
		if workspace == "T2" {
			publicID, dmID = "CPUBLIC2", "DDRAFTONLY"
		}
		value := map[string]any{
			"selfTeamIds": map[string]any{"teamId": workspace}, "bootData": map[string]any{"user_id": "U" + workspace},
			"channels": map[string]any{publicID: map[string]any{"id": publicID, "is_channel": true, "context_team_id": workspace}, dmID: map[string]any{"id": dmID, "is_im": true, "context_team_id": workspace}},
			"messages": map[string]any{publicID: map[string]any{"1": map[string]any{"ts": "1710000010.000001", "text": "desktop public message", "type": "message"}}, dmID: map[string]any{"2": map[string]any{"ts": "1710000020.000001", "text": "desktop-dm-canary <@UDMCANARY>", "type": "message"}}},
		}
		body, err := json.Marshal(value)
		require.NoError(t, err)
		cmd := exec.Command("node", "-e", `process.stdout.write(require("v8").serialize(JSON.parse(require("fs").readFileSync(0,"utf8"))))`)
		cmd.Stdin = strings.NewReader(string(body))
		payload, err := cmd.Output()
		require.NoError(t, err)
		dir := filepath.Join(cfg.Slack.Desktop.Path, "IndexedDB", "https_app.slack.com_0.indexeddb.blob")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, workspace), payload, 0o600))
	}
	return cfg, path
}
