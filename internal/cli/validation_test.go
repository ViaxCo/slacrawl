package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/stretchr/testify/require"
)

func TestAnalyticsSubcommandHelpIgnoresConfig(t *testing.T) {
	for _, command := range []string{"digest", "quiet", "trends"} {
		for _, help := range []string{"--help", "-h"} {
			t.Run(command+help, func(t *testing.T) {
				for _, malformed := range []bool{false, true} {
					configPath := filepath.Join(t.TempDir(), "config.toml")
					if malformed {
						require.NoError(t, os.WriteFile(configPath, []byte("invalid = ["), 0o600))
					}
					var output bytes.Buffer
					app := &App{Stdout: &output, Stderr: &output}
					err := app.Run(context.Background(), []string{"--config", configPath, "analytics", command, help})
					require.NoError(t, err)
					require.Contains(t, output.String(), "Usage")
					flag := "since"
					if command == "trends" {
						flag = "weeks"
					}
					require.Contains(t, output.String(), flag, "help must describe the command flags")
				}
			})
		}
	}
}

func TestSyncRejectsNonFiniteSinceBeforeOpeningStore(t *testing.T) {
	for _, since := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf", "Infinity", "-Infinity"} {
		t.Run(since, func(t *testing.T) {
			root := t.TempDir()
			cfg := config.Default()
			cfg.DBPath = filepath.Join(root, "archive.db")
			cfg.CacheDir = filepath.Join(root, "cache")
			cfg.LogDir = filepath.Join(root, "logs")
			cfg.Slack.Desktop.Enabled = false
			configPath := filepath.Join(root, "config.toml")
			require.NoError(t, cfg.Save(configPath))
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			err := app.Run(context.Background(), []string{"--config", configPath, "sync", "--source", "wiretap", "--since", since})
			require.ErrorContains(t, err, "--since must be a slack timestamp or an RFC3339 time")
			_, err = os.Stat(cfg.DBPath)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
