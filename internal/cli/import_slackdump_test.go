package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlackdumpConvertedExportsFromCLI(t *testing.T) {
	t.Setenv("SLACRAWL_NO_UPDATE_CHECK", "1")
	root := filepath.Join("..", "..", "testdata", "slackdump", "f7319928")
	data, err := os.ReadFile(filepath.Join(root, "provenance.json"))
	require.NoError(t, err)
	var provenance struct {
		Upstream struct{ Commit, Tree string }
		Files    map[string]struct {
			Bytes  int
			SHA256 string
		}
	}
	require.NoError(t, json.Unmarshal(data, &provenance))
	require.Equal(t, "f7319928b0993b23d7e9bd8af5e4c69b6f1d2af4", provenance.Upstream.Commit)
	require.Equal(t, "9b0568bc31804ca8be9d4475d6ce9e54b5fba25f", provenance.Upstream.Tree)
	require.Len(t, provenance.Files, 24)
	seen := map[string]bool{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		if entry.IsDir() {
			return nil
		}
		require.True(t, entry.Type().IsRegular(), path)
		name, err := filepath.Rel(root, path)
		require.NoError(t, err)
		name = filepath.ToSlash(name)
		if name == "provenance.json" {
			return nil
		}
		want, ok := provenance.Files[name]
		require.True(t, ok, name)
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		digest := sha256.Sum256(body)
		require.Len(t, body, want.Bytes, name)
		require.Equal(t, want.SHA256, hex.EncodeToString(digest[:]), name)
		seen[name] = true
		return nil
	}))
	require.Len(t, seen, len(provenance.Files))

	for _, tc := range []struct {
		backend, format, policy string
		repeat                  bool
	}{
		{"database", "directory", "false", false},
		{"database", "zip", "false", true},
		{"chunks", "directory", "false", false},
		{"chunks", "zip", "false", false},
		{"database", "zip", "omitted", false},
		{"database", "zip", "true", false},
	} {
		t.Run(tc.backend+"/"+tc.format+"/"+tc.policy, func(t *testing.T) {
			cfg, configPath := importAdmissionConfig(t, tc.policy)
			configBefore, err := os.ReadFile(configPath)
			require.NoError(t, err)
			export := filepath.Join(root, tc.backend, "export")
			if tc.format == "zip" {
				export += ".zip"
			}
			attempts := 1
			if tc.repeat {
				attempts = 2
			}
			for attempt := range attempts {
				before := importAdmissionSnapshot(t, cfg.DBPath)
				var stdout, stderr bytes.Buffer
				app := &App{Stdout: &stdout, Stderr: &stderr}
				runErr := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", export, "--workspace", "TTEST"})
				after := importAdmissionSnapshot(t, cfg.DBPath)
				configAfter, readErr := os.ReadFile(configPath)
				errorText := ""
				if runErr != nil {
					errorText = runErr.Error()
				}
				observation, err := json.Marshal(map[string]any{
					"attempt": attempt + 1, "error": errorText, "stdout": stdout.String(), "stderr": stderr.String(),
					"before": before, "after": after, "config_preserved": bytes.Equal(configBefore, configAfter),
				})
				require.NoError(t, err)
				t.Logf("slackdump import observation: %s", observation)
				require.NoError(t, runErr)
				require.NoError(t, readErr)
				require.Empty(t, stderr.String())
				require.Equal(t, configBefore, configAfter)
				var report ImportReport
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
				wantMessages, wantDM, wantOmitted := 8, 1, 0
				if tc.policy == "false" {
					wantMessages, wantDM, wantOmitted = 6, 0, 2
				}
				wantSkipped := 0
				if attempt > 0 {
					wantMessages, wantSkipped = 0, 6
				}
				require.Equal(t, "TTEST", report.Workspace)
				require.Equal(t, 2, report.Users)
				require.Equal(t, 3, report.Channels)
				require.Equal(t, wantDM, report.DMs)
				require.Equal(t, wantDM, report.MPIMs)
				require.Equal(t, wantOmitted, report.OmittedDM)
				require.Equal(t, wantMessages, report.Messages)
				require.Equal(t, wantSkipped, report.Skipped)
				require.False(t, report.DryRun)
				require.GreaterOrEqual(t, report.Elapsed.Nanoseconds(), int64(0))
				assertSlackdumpImportSnapshot(t, after, tc.policy == "false")
				if attempt > 0 {
					// Metadata timestamps may advance; repeated messages and their derived rows must not.
					for _, table := range []string{"messages", "message_events", "message_event_heads", "message_fts"} {
						require.Equal(t, before[table], after[table], table)
					}
				}
			}
		})
	}
}

func assertSlackdumpImportSnapshot(t *testing.T, snapshot map[string][]map[string]any, strict bool) {
	t.Helper()
	require.Len(t, snapshot, 11)
	require.Len(t, snapshot["workspaces"], 1)
	require.Equal(t, "TTEST", snapshot["workspaces"][0]["id"])
	users := map[any]any{}
	for _, row := range snapshot["users"] {
		require.Equal(t, "TTEST", row["workspace_id"])
		users[row["id"]] = row["name"]
	}
	require.Len(t, snapshot["users"], 2)
	require.Equal(t, map[any]any{"UME": "me", "UOTHER": "other"}, users)
	modernKind := "public_channel"
	if strict {
		modernKind = "private_channel"
	}
	wantChannels := map[any][]any{
		"CPUBLIC": {"public", "public_channel", int64(0)},
		"GLEGACY": {"legacy", "private_channel", int64(1)},
		"CMODERN": {"modern", modernKind, int64(1)},
	}
	if !strict {
		wantChannels["DDIRECT"] = []any{"DDIRECT", "im", int64(1)}
		wantChannels["GMULTI"] = []any{"multi", "mpim", int64(1)}
	}
	channels := map[any][]any{}
	for _, row := range snapshot["channels"] {
		require.Equal(t, "TTEST", row["workspace_id"])
		channels[row["id"]] = []any{row["name"], row["kind"], row["is_private"]}
	}
	require.Len(t, snapshot["channels"], len(wantChannels))
	require.Equal(t, wantChannels, channels)
	const parent = "1767225600.000001"
	wantMessages := map[string][2]string{
		"CPUBLIC|" + parent:         {"slackdump-public-parent", parent},
		"CPUBLIC|1767225600.000002": {"slackdump-public-reply-one", parent},
		"CPUBLIC|1767312000.000001": {"slackdump-public-reply-two", parent},
		"CPUBLIC|1767312000.000002": {"slackdump-public-standalone", ""},
		"GLEGACY|" + parent:         {"slackdump-legacy-canary", ""},
		"CMODERN|" + parent:         {"slackdump-modern-canary", ""},
	}
	if !strict {
		wantMessages["DDIRECT|"+parent] = [2]string{"slackdump-dm-canary", ""}
		wantMessages["GMULTI|"+parent] = [2]string{"slackdump-mpim-canary", ""}
	}
	rawMessages, normalized := map[string]any{}, map[any]any{}
	require.Len(t, snapshot["messages"], len(wantMessages))
	for _, row := range snapshot["messages"] {
		key := row["channel_id"].(string) + "|" + row["ts"].(string)
		want, ok := wantMessages[key]
		require.True(t, ok, key)
		require.Equal(t, "TTEST", row["workspace_id"])
		require.Equal(t, "UME", row["user_id"])
		require.Equal(t, want[0], row["text"])
		norm := want[0]
		if want[1] == "" {
			require.Nil(t, row["thread_ts"])
		} else {
			require.Equal(t, want[1], row["thread_ts"])
			if want[1] != row["ts"] {
				norm += " [thread-reply]"
			}
		}
		require.Equal(t, norm, row["normalized_text"])
		require.Equal(t, int64(2), row["source_rank"])
		require.Equal(t, "slack-export", row["source_name"])
		require.Nil(t, row["deleted_ts"])
		require.Nil(t, row["subtype"])
		if key == "CPUBLIC|"+parent {
			require.Equal(t, int64(2), row["reply_count"])
			require.Equal(t, "1767312000.000001", row["latest_reply"])
		}
		var raw map[string]any
		require.NoError(t, json.Unmarshal([]byte(row["raw_json"].(string)), &raw))
		require.Equal(t, row["ts"], raw["ts"])
		require.Equal(t, want[0], raw["text"])
		rawMessages[key], normalized[key] = row["raw_json"], norm
	}
	for _, table := range []string{"message_events", "message_event_heads"} {
		payloads := map[string]any{}
		require.Len(t, snapshot[table], len(wantMessages), table)
		for _, row := range snapshot[table] {
			key := row["channel_id"].(string) + "|" + row["ts"].(string)
			require.Equal(t, "slack-export", row["source_name"])
			require.Equal(t, "message", row["event_type"])
			payloads[key] = row["payload_json"]
		}
		require.Equal(t, rawMessages, payloads, table)
	}
	fts := map[any]any{}
	require.Len(t, snapshot["message_fts"], len(wantMessages))
	for _, row := range snapshot["message_fts"] {
		fts[row["message_key"]] = row["content"]
	}
	require.Equal(t, normalized, fts)
	for _, table := range []string{"message_files", "message_mentions", "sync_state", "embedding_jobs"} {
		require.Empty(t, snapshot[table], table)
	}
	serialized, err := json.Marshal(snapshot)
	require.NoError(t, err)
	for _, canary := range []string{"DDIRECT", "GMULTI", "slackdump-dm-canary", "slackdump-mpim-canary"} {
		if strict {
			require.NotContains(t, string(serialized), canary)
		} else {
			require.Contains(t, string(serialized), canary)
		}
	}
}
