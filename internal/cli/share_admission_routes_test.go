package cli

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestShareAdmissionAcrossAutomaticCLIPaths(t *testing.T) {
	dir := t.TempDir()
	isolateShareGit(t, dir)
	routes := []struct {
		name string
		args []string
		read bool
	}{
		{"search", []string{"search", "local"}, true},
		{"messages", []string{"messages"}, true},
		{"mentions", []string{"mentions"}, true},
		{"sql", []string{"sql", "select count(*) as n from messages"}, true},
		{"users", []string{"users"}, true},
		{"channels", []string{"channels"}, true},
		{"report", []string{"report"}, true},
		{"digest", []string{"digest"}, true},
		{"analytics-digest", []string{"analytics", "digest"}, true},
		{"analytics-quiet", []string{"analytics", "quiet"}, true},
		{"analytics-trends", []string{"analytics", "trends"}, true},
		{"files", []string{"files"}, true},
		{"files-fetch", []string{"files", "fetch"}, false},
		{"tail", []string{"tail"}, false},
	}
	for _, source := range []string{"api", "bot", "all", "hybrid", "mcp", "connector", "provider:fixture"} {
		routes = append(routes, struct {
			name string
			args []string
			read bool
		}{"sync-" + source, []string{"sync", "--source", source}, false})
	}
	for _, route := range routes {
		modes := []string{"stale"}
		if route.read {
			modes = append(modes, "auto-off", "share-disabled")
		}
		if route.name == "search" {
			modes = append(modes, "missing-timestamp", "invalid-timestamp")
		}
		for _, mode := range modes {
			t.Run(route.name+"/"+mode, func(t *testing.T) {
				dir := t.TempDir()
				cfg := shareAdmissionConfig(dir)
				cfg.Share.Remote = filepath.Join(dir, "unacquired-remote.git")
				cfg.Sync.IncludeDMs = new(false)
				if mode == "auto-off" {
					cfg.Share.AutoUpdate = false
				} else if mode == "share-disabled" {
					cfg.Share.Remote = ""
				}
				path := filepath.Join(dir, "config.toml")
				require.NoError(t, cfg.Save(path))
				seedArchiveStore(t, cfg.DBPath, "local marker")
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				if mode != "missing-timestamp" {
					lastImport := "2000-01-01T00:00:00Z"
					if mode == "invalid-timestamp" {
						lastImport = "invalid"
					}
					require.NoError(t, st.SetSyncState(context.Background(), "share", "import", "last_import_at", lastImport))
				}
				require.NoError(t, st.Close())
				before := shareArchiveSnapshot(t, cfg.DBPath)
				configBody, err := os.ReadFile(path)
				require.NoError(t, err)
				var output bytes.Buffer
				var httpCalls atomic.Int32
				client := &http.Client{Transport: cliRoundTripFunc(func(*http.Request) (*http.Response, error) {
					httpCalls.Add(1)
					return nil, errors.New("unexpected fixture HTTP request")
				})}
				app := &App{Stdout: &output, Stderr: &output, httpClient: client}
				trace := filepath.Join(dir, "trace.jsonl")
				t.Setenv("GIT_TRACE2_EVENT", trace)
				err = app.Run(context.Background(), append([]string{"--config", path, "--json"}, route.args...))
				starts := shareGitStarts(t, trace)
				t.Setenv("GIT_TRACE2_EVENT", "")
				if mode != "auto-off" && mode != "share-disabled" {
					require.ErrorContains(t, err, "sync.include_dms=false")
					require.ErrorContains(t, err, "share.auto_update=false")
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, output.String())
				}
				require.Zero(t, starts)
				require.Zero(t, httpCalls.Load())
				require.Equal(t, before, shareArchiveSnapshot(t, cfg.DBPath))
				afterConfig, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, configBody, afterConfig)
				require.NoDirExists(t, cfg.Share.RepoPath)
				require.NotContains(t, output.String(), shareDMCanary)
			})
		}
	}
}

func TestShareAdmissionPreservesObservationAndDesktopPaths(t *testing.T) {
	dir := t.TempDir()
	isolateShareGit(t, dir)
	for _, route := range []string{"status", "desktop", "wiretap", "watch"} {
		t.Run(route, func(t *testing.T) {
			cfg, path := desktopDraftConfig(t, "false")
			cfg.Sync.IncludeDMs = new(false)
			cfg.Share.Remote = filepath.Join(filepath.Dir(path), "unacquired-remote.git")
			cfg.Share.AutoUpdate = true
			require.NoError(t, cfg.Save(path))
			seedArchiveStore(t, cfg.DBPath, "local marker")
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			const oldImport = "2000-01-01T00:00:00Z"
			require.NoError(t, st.SetSyncState(context.Background(), "share", "import", "last_import_at", oldImport))
			require.NoError(t, st.Close())
			before := shareArchiveSnapshot(t, cfg.DBPath)
			args := []string{"sync", "--source", route}
			if route == "status" {
				args = []string{"status"}
			} else if route == "watch" {
				args = []string{"watch", "--desktop-every", "1h"}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			if route == "watch" {
				app.Stdout = &cancelAfterWrite{Writer: &output, cancel: cancel}
			}
			trace := filepath.Join(filepath.Dir(path), "trace.jsonl")
			t.Setenv("GIT_TRACE2_EVENT", trace)
			err = app.Run(ctx, append([]string{"--config", path, "--json"}, args...))
			starts := shareGitStarts(t, trace)
			t.Setenv("GIT_TRACE2_EVENT", "")
			if route == "watch" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, starts)
			require.NoDirExists(t, cfg.Share.RepoPath)
			after := shareArchiveSnapshot(t, cfg.DBPath)
			if route == "status" {
				require.Equal(t, before, after)
			} else {
				require.Contains(t, output.String(), "summary")
			}
			st, err = store.OpenReadOnly(cfg.DBPath)
			require.NoError(t, err)
			last, err := st.GetSyncState(context.Background(), "share", "import", "last_import_at")
			require.NoError(t, err)
			require.Equal(t, oldImport, last)
			require.NoError(t, st.Close())
		})
	}
}

func TestShareAdmissionAutoOffAllowsAPISync(t *testing.T) {
	dir := t.TempDir()
	isolateShareGit(t, dir)
	cfg := shareAdmissionConfig(dir)
	cfg.WorkspaceID = "T1"
	cfg.Sync.IncludeDMs = new(false)
	cfg.Share.Remote = filepath.Join(dir, "unacquired-remote.git")
	cfg.Share.AutoUpdate = false
	cfg.Slack.Bot.Enabled = true
	cfg.Slack.Bot.TokenEnv = "SLACRAWL_SHARE_FIXTURE_TOKEN"
	t.Setenv(cfg.Slack.Bot.TokenEnv, "xoxb-share-fixture")
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, cfg.Save(path))
	server := newCLIFanoutSlackServer(t, map[string]string{"xoxb-share-fixture": "T1"})
	defer server.Close()
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output, apiURL: server.URL + "/", httpClient: server.Client()}
	trace := filepath.Join(dir, "trace.jsonl")
	t.Setenv("GIT_TRACE2_EVENT", trace)
	err := app.Run(context.Background(), []string{"--config", path, "--json", "sync", "--source", "bot", "--full"})
	starts := shareGitStarts(t, trace)
	t.Setenv("GIT_TRACE2_EVENT", "")
	require.NoError(t, err)
	require.Zero(t, starts)
	require.NoDirExists(t, cfg.Share.RepoPath)
	st, err := store.OpenReadOnly(cfg.DBPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	rows, err := st.QueryReadOnly(context.Background(), "select channel_id from messages")
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"channel_id": "C111"}}, rows)
	rows, err = st.QueryReadOnly(context.Background(), "select * from sync_state where source_name = 'share'")
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestShareAdmissionPreservesArgumentErrorsAndLocalUpdateGate(t *testing.T) {
	dir := t.TempDir()
	isolateShareGit(t, dir)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"missing-remote", []string{"subscribe"}, "subscribe requires a remote"},
		{"extra-remote", []string{"subscribe", "one", "two"}, "subscribe takes at most one remote"},
		{"update-argument", []string{"update", "extra"}, "update takes no positional arguments"},
		{"historical-mode", []string{"update", "--ref", "baseline"}, "update --ref requires --restore"},
		{"local-update", []string{"update"}, "sync.include_dms=false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := shareAdmissionConfig(dir)
			cfg.Sync.IncludeDMs = new(false)
			path := filepath.Join(dir, "config.toml")
			require.NoError(t, cfg.Save(path))
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			trace := filepath.Join(dir, "trace.jsonl")
			t.Setenv("GIT_TRACE2_EVENT", trace)
			err = app.Run(context.Background(), append([]string{"--config", path, "--json"}, tc.args...))
			starts := shareGitStarts(t, trace)
			t.Setenv("GIT_TRACE2_EVENT", "")
			require.ErrorContains(t, err, tc.want)
			if tc.name != "local-update" {
				require.False(t, strings.Contains(err.Error(), "sync.include_dms"))
			}
			require.Zero(t, starts)
			require.NoFileExists(t, cfg.DBPath)
			require.NoDirExists(t, cfg.Share.RepoPath)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
