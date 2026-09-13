package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestProviderDMPolicyFromCLIConfig(t *testing.T) {
	for _, policy := range []string{"omitted", "false", "true"} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing=%t", policy, existing), func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				launchPath, requestPath := filepath.Join(root, "launched"), filepath.Join(root, "request.json")
				cfg := config.Default()
				cfg.DBPath = filepath.Join(root, "archive", "archive.db")
				cfg.CacheDir, cfg.LogDir = filepath.Join(root, "cache"), filepath.Join(root, "logs")
				cfg.Share.Remote, cfg.Share.RepoPath, cfg.Share.AutoUpdate = "", filepath.Join(root, "share"), false
				cfg.Slack.Bot.Enabled, cfg.Slack.User.Enabled, cfg.Slack.App.Enabled = false, false, false
				cfg.Slack.Desktop.Enabled, cfg.Slack.Desktop.Path = false, filepath.Join(root, "desktop")
				includeDrafts := false
				cfg.Slack.Desktop.IncludeDrafts = &includeDrafts
				cfg.Slack.MCP.Enabled, cfg.Slack.MCP.AuthPath = false, filepath.Join(root, "auth.json")
				if policy != "omitted" {
					include := policy == "true"
					cfg.Sync.IncludeDMs = &include
				}
				executable, err := os.Executable()
				require.NoError(t, err)
				cfg.Providers = []config.Provider{{
					Name: "fixture", Command: executable, SourceRank: 5, BatchSize: 7,
					Args: []string{"-test.run=^TestProviderDMPolicyHelper$", "--", "provider-dm-policy", launchPath, requestPath},
				}}
				configPath := filepath.Join(root, "config.toml")
				require.NoError(t, cfg.Save(configPath))
				if existing {
					seedArchiveStore(t, cfg.DBPath, "retained local message")
					st, err := store.Open(cfg.DBPath)
					require.NoError(t, err)
					require.NoError(t, st.SetSyncState(ctx, "provider:fixture", "workspace", "TPROVIDER", "seed-old-cursor"))
					require.NoError(t, st.SetSyncState(ctx, "provider:fixture", "workspace_scope", "TPROVIDER|retained-scope", "seed-scoped-cursor"))
					require.NoError(t, st.Close())
				}
				before := shareArchiveSnapshot(t, cfg.DBPath)
				configBefore, err := os.ReadFile(configPath)
				require.NoError(t, err)
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output}
				runErr := app.Run(ctx, []string{"--config", configPath, "--json", "sync", "--source", "provider:fixture", "--workspace", "TPROVIDER", "--with-media=false"})
				after := shareArchiveSnapshot(t, cfg.DBPath)
				configAfter, err := os.ReadFile(configPath)
				require.NoError(t, err)
				launched := sharePathExists(t, launchPath)
				sent, err := os.ReadFile(requestPath)
				if !os.IsNotExist(err) {
					require.NoError(t, err)
				}
				wantRequest := `{"type":"request","protocol":"slacrawl-provider-v1","workspace_id":"TPROVIDER",`
				if existing {
					wantRequest += `"checkpoint":"seed-old-cursor",`
				}
				wantRequest += "\"batch_size\":7}\n"
				requestMatches := string(sent) == wantRequest
				var report struct {
					Summary struct {
						Provider struct {
							Records, Messages, Checkpoints int
							MessagesWritten                int `json:"messages_written"`
						} `json:"provider"`
					} `json:"summary"`
				}
				decodeErr := json.Unmarshal(output.Bytes(), &report)
				providerRows, dmRows, unknownChannels := 0, 0, 0
				for _, row := range after["messages"] {
					if row["source_name"] == "provider:fixture" {
						providerRows++
					}
					if row["channel_id"] == "DIM" || row["channel_id"] == "GMPIM" {
						dmRows++
					}
				}
				for _, row := range after["channels"] {
					if row["kind"] == "provider_channel" {
						unknownChannels++
					}
				}
				checkpoint := false
				for _, row := range after["sync_state"] {
					if row["source_name"] == "provider:fixture" && row["entity_type"] == "workspace" && row["entity_id"] == "TPROVIDER" && row["value"] == "provider-policy-checkpoint" {
						checkpoint = true
					}
				}
				archivePreserved := true
				for table, rows := range after {
					if len(rows) != len(before[table]) || (len(rows) > 0 && !reflect.DeepEqual(rows, before[table])) {
						archivePreserved = false
					}
				}
				configPreserved := bytes.Equal(configBefore, configAfter)
				diagnostics := output.String()
				if runErr != nil {
					diagnostics += runErr.Error()
				}
				outputLeak := strings.Contains(diagnostics, "provider-policy-")
				policyError := runErr != nil && runErr.Error() == "sync workspace TPROVIDER: external provider v1 cannot enforce sync.include_dms=false; use API sync or a supported Slack workspace JSON export"
				t.Logf("provider admission observation: error=%t launched=%t records=%d messages=%d provider_rows=%d dm_rows=%d unknown_channels=%d checkpoint=%t request=%t archive_preserved=%t config_preserved=%t output_leak=%t", runErr != nil, launched, report.Summary.Provider.Records, report.Summary.Provider.Messages, providerRows, dmRows, unknownChannels, checkpoint, requestMatches, archivePreserved, configPreserved, outputLeak)
				if policy == "false" {
					if !policyError || launched || len(sent) != 0 || providerRows != 0 || dmRows != 0 || checkpoint || !archivePreserved || !configPreserved || outputLeak {
						t.Fatalf("provider admission boundary: error=%t policy_error=%t launched=%t provider_rows=%d dm_rows=%d checkpoint=%t archive_preserved=%t config_preserved=%t output_leak=%t", runErr != nil, policyError, launched, providerRows, dmRows, checkpoint, archivePreserved, configPreserved, outputLeak)
					}
					serialized, err := json.Marshal(after)
					require.NoError(t, err)
					require.NotContains(t, string(serialized), "provider-policy-")
					require.Empty(t, output.String())
					return
				}
				require.NoError(t, runErr)
				require.NoError(t, decodeErr)
				require.True(t, launched)
				require.Equal(t, wantRequest, string(sent))
				require.Equal(t, 12, report.Summary.Provider.Records)
				require.Equal(t, 5, report.Summary.Provider.Messages)
				require.Equal(t, 5, report.Summary.Provider.MessagesWritten)
				require.Equal(t, 1, report.Summary.Provider.Checkpoints)
				require.Equal(t, 5, providerRows)
				require.Equal(t, 2, dmRows)
				require.Equal(t, 1, unknownChannels)
				require.True(t, checkpoint)
				require.True(t, configPreserved)
				require.False(t, outputLeak)
				for _, channel := range providerPolicyChannels() {
					var storedChannel, storedMessage map[string]any
					for _, row := range after["channels"] {
						if row["id"] == channel.id {
							storedChannel = row
						}
					}
					for _, row := range after["messages"] {
						if row["channel_id"] == channel.id {
							storedMessage = row
						}
					}
					require.NotNil(t, storedChannel)
					require.NotNil(t, storedMessage)
					wantKind := channel.kind
					if wantKind == "" {
						wantKind = "provider_channel"
					}
					require.Equal(t, wantKind, storedChannel["kind"])
					require.Equal(t, "TPROVIDER", storedChannel["workspace_id"])
					require.Equal(t, "provider:fixture", storedMessage["source_name"])
					require.Equal(t, int64(5), storedMessage["source_rank"])
					require.Equal(t, `{"marker":"provider-policy-raw-`+channel.id+`"}`, storedMessage["raw_json"])
					require.Contains(t, storedMessage["text"], "provider-policy-text-"+channel.id)
					for _, table := range []string{"message_mentions", "message_events", "message_event_heads", "message_fts"} {
						rows := 0
						for _, row := range after[table] {
							if row["channel_id"] == channel.id || (table == "message_fts" && row["message_key"] == channel.id+"|1710000001.000001") {
								rows++
								if table == "message_mentions" {
									require.Equal(t, "provider-policy-mention-"+channel.id, row["display_text"])
								}
							}
						}
						require.Equal(t, 1, rows, table)
					}
				}
				for table, rows := range before {
					for _, row := range rows {
						if table == "sync_state" && row["entity_type"] == "workspace" && row["entity_id"] == "TPROVIDER" {
							continue // Only the selected provider checkpoint advances.
						}
						require.Contains(t, after[table], row, table)
					}
				}
			})
		}
	}
}

func providerPolicyChannels() []struct{ id, kind string } {
	return []struct{ id, kind string }{{"CPUBLIC", "public_channel"}, {"CPRIVATE", "private_channel"}, {"DIM", "im"}, {"GMPIM", "mpim"}, {"CUNKNOWN", ""}}
}

func TestProviderDMPolicyHelper(t *testing.T) {
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		return
	}
	if len(os.Args) != separator+4 || os.Args[separator+1] != "provider-dm-policy" {
		os.Exit(2)
	}
	// Mark launch before reading stdin, so an early child still counts as intake.
	if err := os.WriteFile(os.Args[separator+2], []byte("launched"), 0o600); err != nil {
		os.Exit(3)
	}
	sent, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(4)
	}
	if err := os.WriteFile(os.Args[separator+3], sent, 0o600); err != nil {
		os.Exit(5)
	}
	emit := func(value any) {
		if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
			os.Exit(6)
		}
	}
	fmt.Fprintln(os.Stderr, "provider-policy-stderr")
	emit(map[string]any{"type": "hello", "protocol": "slacrawl-provider-v1", "provider": "fixture"})
	emit(map[string]any{"type": "workspace", "id": "TPROVIDER", "name": "synthetic workspace"})
	emit(map[string]any{"type": "user", "workspace_id": "TPROVIDER", "id": "UPROVIDER", "name": "synthetic user"})
	for _, channel := range providerPolicyChannels() {
		emit(map[string]any{"type": "channel", "workspace_id": "TPROVIDER", "id": channel.id, "name": channel.id, "kind": channel.kind})
		emit(map[string]any{
			"type": "message", "workspace_id": "TPROVIDER", "channel_id": channel.id, "ts": "1710000001.000001", "user_id": "UPROVIDER",
			"text":     "provider-policy-text-" + channel.id + " <@UMENTION|provider-policy-mention-" + channel.id + ">",
			"raw_json": map[string]any{"marker": "provider-policy-raw-" + channel.id},
		})
	}
	emit(map[string]any{"type": "checkpoint", "entity_type": "workspace", "entity_id": "TPROVIDER", "value": "provider-policy-checkpoint"})
	emit(map[string]any{"type": "done", "records": 12})
	os.Exit(0)
}
