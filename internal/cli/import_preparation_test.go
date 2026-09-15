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

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/importer"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestImportExcludedBodiesAndAllExcluded(t *testing.T) {
	for _, format := range []string{"zip", "directory"} {
		for _, existing := range []bool{false, true} {
			for _, allExcluded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/existing=%t/all=%t", format, existing, allExcluded), func(t *testing.T) {
					cfg, configPath := importAdmissionConfig(t, "false")
					if existing {
						seedImportArchive(t, cfg.DBPath)
					}
					before := importAdmissionSnapshot(t, cfg.DBPath)
					files := importAdmissionFiles()
					files["direct/2026-01-01.json"] = "malformed excluded DM body canary"
					files["multi/2026-01-01.json"] = "malformed excluded MPIM body canary"
					if allExcluded {
						delete(files, "channels.json")
						delete(files, "groups.json")
					}
					source := importAdmissionExport(t, format, files)
					var output bytes.Buffer
					app := &App{Stdout: &output, Stderr: &output}
					for range 2 {
						output.Reset()
						require.NoError(t, app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT", "--force"}))
						require.NotContains(t, output.String(), "canary")
						if allExcluded {
							require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
							require.False(t, importAdmissionExists(t, cfg.CacheDir))
							require.False(t, importAdmissionExists(t, cfg.LogDir))
							if !existing {
								require.False(t, importAdmissionExists(t, cfg.DBPath))
							}
						}
					}
					cfg.Sync.IncludeDMs = new(true)
					require.NoError(t, cfg.Save(configPath))
					output.Reset()
					err := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
					require.Error(t, err)
					require.NotContains(t, err.Error(), "canary")
				})
			}
		}
	}
}

func TestImportCatalogAndLocatorErrorsBeforeStore(t *testing.T) {
	for _, format := range []string{"zip", "directory"} {
		for _, mode := range []string{"name-id", "unknown", "null", "sparse-conflict", "missing-id", "unsupported", "latest-identity"} {
			t.Run(format+"/"+mode, func(t *testing.T) {
				cfg, configPath := importAdmissionConfig(t, "false")
				files := map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`}
				switch mode {
				case "name-id":
					files["dms.json"] = `[{"id":"D1","name":"C1"}]`
				case "unknown":
					files["channels.json"] = `[{"id":"C1","name":"room","is_im":false}]`
				case "null":
					files["channels.json"] = `[{"id":"C1","name":"room","is_channel":null}]`
				case "sparse-conflict":
					files["channels.json"] = `[{"id":"C1","name":"room","is_private":true}]`
				case "missing-id":
					files["channels.json"] = `[{"id":"  ","name":"room"}]`
				case "unsupported":
					files = map[string]string{"unrelated.json": "[]"}
				case "latest-identity":
					files["channels.json"] = `[{"id":"C1","name":"room","latest":{"channel":"CFORBIDDEN"}}]`
				}
				source := importAdmissionExport(t, format, files)
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output}
				err := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
				require.Error(t, err)
				require.NotContains(t, err.Error(), "CFORBIDDEN")
				require.False(t, importAdmissionExists(t, cfg.DBPath))
				require.False(t, importAdmissionExists(t, cfg.CacheDir))
				require.False(t, importAdmissionExists(t, cfg.LogDir))
			})
		}
	}
}

func TestImportNativeCatalogTypesFromCLI(t *testing.T) {
	for _, format := range []string{"zip", "directory"} {
		for _, policy := range []string{"omitted", "false", "true"} {
			t.Run(format+"/"+policy, func(t *testing.T) {
				cfg, configPath := importAdmissionConfig(t, policy)
				files := importAdmissionFiles()
				files["channels.json"] = `[{"id":"CPUBLIC","name":"public","is_channel":true,"is_private":true},{"id":"CDIRECT","name":"direct","is_channel":true},{"id":"CMULTI","name":"multi","is_mpim":true}]`
				files["groups.json"] = `[{"id":"CPRIVATE","name":"private","is_channel":true,"is_private":false}]`
				var output bytes.Buffer
				source := importAdmissionExport(t, format, files)
				app := &App{Stdout: &output, Stderr: &output}
				require.NoError(t, app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"}))
				rows := importAdmissionSnapshot(t, cfg.DBPath)
				wantDM := 2
				if policy == "false" {
					wantDM = 0
				}
				require.Equal(t, wantDM, importAdmissionCount(rows["messages"], "CDIRECT")+importAdmissionCount(rows["messages"], "CMULTI"))
				channels := map[string]map[string]any{}
				for _, row := range rows["channels"] {
					channels[row["id"].(string)] = row
				}
				for _, id := range []string{"CPUBLIC", "CPRIVATE"} {
					require.Contains(t, channels, id)
					require.Equal(t, 1, importAdmissionCount(rows["messages"], id), id)
				}
				if policy == "false" {
					require.Equal(t, "private_channel", channels["CPUBLIC"]["kind"])
					require.Equal(t, "public_channel", channels["CPRIVATE"]["kind"])
				}
			})
		}
	}
}

func TestImportIdentityPreflightIncludesSkippedRows(t *testing.T) {
	for _, policy := range []string{"omitted", "false", "true"} {
		for _, mode := range []string{"blank-ts", "priority", "late-collision", "dry-collision"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				cfg, configPath := importAdmissionConfig(t, policy)
				seedImportArchive(t, cfg.DBPath)
				st, err := store.Open(cfg.DBPath)
				require.NoError(t, err)
				now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				msg, mentions, _ := toStoreMessage("TOTHER", "C1", map[string]any{"ts": "2", "text": "old"}, now)
				if mode == "priority" {
					msg.WorkspaceID = "TEXPORT"
					msg.SourceName = "api-user"
					msg.SourceRank = 1
				}
				_, err = st.ApplyWriteBatch(context.Background(), store.WriteBatch{Messages: []store.MessageWrite{{Message: msg, Mentions: mentions}}})
				require.NoError(t, err)
				require.NoError(t, st.Close())
				before := importAdmissionSnapshot(t, cfg.DBPath)
				bad := `{"ts":"2","channel":"CFORBIDDEN","text":"bad-identity-canary"}`
				if mode == "blank-ts" {
					bad = `{"channel":"CFORBIDDEN","text":"bad-identity-canary"}`
				}
				if mode == "late-collision" || mode == "dry-collision" {
					bad = `{"ts":"2","text":"collision"}`
				}
				source := importAdmissionExport(t, "zip", map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": `[{"ts":"1","text":"would-write"},` + bad + "]"})
				args := []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"}
				if mode == "dry-collision" {
					args = append(args, "--dry-run")
				}
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output}
				err = app.Run(context.Background(), args)
				require.Error(t, err)
				require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
				require.NotContains(t, output.String()+err.Error(), "CFORBIDDEN")
				require.NotContains(t, output.String()+err.Error(), "bad-identity-canary")
			})
		}
	}
}

func TestImportExcludedArchiveRowsRemainUnchanged(t *testing.T) {
	cfg, configPath := importAdmissionConfig(t, "false")
	seedImportArchive(t, cfg.DBPath)
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, st.UpsertWorkspace(context.Background(), store.Workspace{ID: "TOTHER", Name: "prior", RawJSON: "{}", UpdatedAt: now}))
	for _, id := range []string{"CDIRECT", "CMULTI"} {
		require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: id, WorkspaceID: "TOTHER", Name: id, Kind: "im", RawJSON: "{}", UpdatedAt: now}))
		raw := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(importAdmissionMessage("3.000001", "old-"+id)), &raw))
		message, mentions, _ := toStoreMessage("TOTHER", id, raw, now)
		_, err := st.ApplyWriteBatch(context.Background(), store.WriteBatch{Messages: []store.MessageWrite{{Message: message, Mentions: mentions}}})
		require.NoError(t, err)
		require.NoError(t, st.SetSyncState(context.Background(), "api-user", "channel", id, "old-state"))
		_, err = st.DB().Exec("insert into embedding_jobs(channel_id,ts,state,created_at) values(?,?,?,?)", id, message.TS, "pending", "old")
		require.NoError(t, err)
	}
	require.NoError(t, st.Close())
	before := importAdmissionSnapshot(t, cfg.DBPath)
	source := importAdmissionExport(t, "zip", importAdmissionFiles())
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output}
	for range 2 {
		require.NoError(t, app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT", "--force"}))
		after := importAdmissionSnapshot(t, cfg.DBPath)
		for _, table := range []string{"channels", "messages", "message_files", "message_mentions", "message_events", "message_event_heads", "message_fts", "sync_state", "embedding_jobs"} {
			require.Equal(t, importExcludedRows(before[table]), importExcludedRows(after[table]), table)
		}
	}
}

func importExcludedRows(rows []map[string]any) []map[string]any {
	result := []map[string]any{}
	for _, row := range rows {
		blob, _ := json.Marshal(row)
		if bytes.Contains(blob, []byte("CDIRECT")) || bytes.Contains(blob, []byte("CMULTI")) {
			result = append(result, row)
		}
	}
	return result
}

func TestImportPriorityAndForce(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			cfg, configPath := importAdmissionConfig(t, "false")
			seedImportArchive(t, cfg.DBPath)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			bodies := []string{}
			for i, source := range []struct {
				name string
				rank int
			}{{"api-user", 1}, {"api-bot", 2}, {"slack-export", 2}, {"desktop", 3}} {
				ts := fmt.Sprintf("%d.000001", i+1)
				bodies = append(bodies, importAdmissionMessage(ts, "incoming"))
				raw := map[string]any{}
				require.NoError(t, json.Unmarshal([]byte(importAdmissionMessage(ts, "prior-"+source.name)), &raw))
				message, mentions, _ := toStoreMessage("TEXPORT", "C1", raw, now)
				message.SourceName = source.name
				message.SourceRank = source.rank
				_, err := st.ApplyWriteBatch(context.Background(), store.WriteBatch{Messages: []store.MessageWrite{{Message: message, Mentions: mentions}}})
				require.NoError(t, err)
			}
			require.NoError(t, st.Close())
			before := importAdmissionSnapshot(t, cfg.DBPath)
			export := importAdmissionExport(t, "zip", map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": "[" + strings.Join(bodies, ",") + "]"})
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			args := []string{"--config", configPath, "--json", "import", export, "--workspace", "TEXPORT"}
			if force {
				args = append(args, "--force")
			}
			require.NoError(t, app.Run(context.Background(), args))
			var report ImportReport
			require.NoError(t, json.Unmarshal(output.Bytes(), &report))
			want := 1
			if force {
				want = 2
			}
			require.Equal(t, want, report.Messages)
			require.Equal(t, 4-want, report.Skipped)
			after := importAdmissionSnapshot(t, cfg.DBPath)
			updated := []string{"4.000001"}
			if force {
				updated = append(updated, "3.000001")
			}
			for _, ts := range updated {
				rows := importRowsAt(after["messages"], ts)
				require.Len(t, rows, 1)
				require.Contains(t, rows[0]["text"], "incoming")
				require.Equal(t, "slack-export", rows[0]["source_name"])
			}
			for _, ts := range []string{"1.000001", "2.000001", "3.000001"} {
				if force && ts == "3.000001" {
					continue
				}
				for _, table := range []string{"messages", "message_files", "message_mentions", "message_events", "message_event_heads", "message_fts"} {
					require.Equal(t, importRowsAt(before[table], ts), importRowsAt(after[table], ts), table, ts)
				}
			}
		})
	}
}

func importRowsAt(rows []map[string]any, ts string) []map[string]any {
	result := []map[string]any{}
	for _, row := range rows {
		if row["ts"] == ts || row["message_key"] == "C1|"+ts {
			result = append(result, row)
		}
	}
	return result
}

func TestImportDryRunDoesNotRepairPendingIndex(t *testing.T) {
	cfg, configPath := importAdmissionConfig(t, "false")
	seedImportArchive(t, cfg.DBPath)
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	require.NoError(t, st.SetSyncState(context.Background(), "store", "search_index", "rowid_rebuild_pending", "1"))
	_, err = st.DB().Exec("delete from message_fts")
	require.NoError(t, err)
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output}
	source := importAdmissionExport(t, "zip", importAdmissionFiles())
	err = app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT", "--dry-run"})
	require.ErrorContains(t, err, "search index rebuild is pending")
	var count int
	require.NoError(t, st.DB().QueryRow("select count(*) from message_fts").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, st.DB().QueryRow("select count(*) from sync_state where entity_id='rowid_rebuild_pending'").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, st.Close())
	require.False(t, importAdmissionExists(t, cfg.CacheDir))
	require.False(t, importAdmissionExists(t, cfg.LogDir))
}

func TestImportPreparedFilesKeepCommittedBatchesOnLateFailure(t *testing.T) {
	for _, mutation := range []string{"changed", "replaced", "missing"} {
		t.Run(mutation, func(t *testing.T) {
			cfg, _ := importAdmissionConfig(t, "false")
			seedImportArchive(t, cfg.DBPath)
			bodies := make([]string, 501)
			for i := range bodies {
				bodies[i] = importAdmissionMessage(fmt.Sprintf("%d.000001", i+1), "kept")
			}
			files := map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": "[" + strings.Join(bodies, ",") + "]", "room/2.json": "[" + importAdmissionMessage("502.000001", "later") + "]"}
			root := importAdmissionExport(t, "directory", files)
			ex, err := importer.Open(root)
			require.NoError(t, err)
			defer ex.Close()
			plan, err := ex.Prepare("TEXPORT", admission.Exclude)
			require.NoError(t, err)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			require.NoError(t, validateImportMessageKeys(context.Background(), st, plan, "TEXPORT"))
			later := filepath.Join(root, "room", "2.json")
			switch mutation {
			case "changed":
				require.NoError(t, os.WriteFile(later, []byte("changed after preflight"), 0600))
			case "replaced":
				replacement := filepath.Join(root, "replacement")
				require.NoError(t, os.WriteFile(replacement, []byte(files["room/2.json"]), 0600))
				require.NoError(t, os.Rename(replacement, later))
			case "missing":
				require.NoError(t, os.Remove(later))
			}
			_, _, err = runImportExecution(context.Background(), st, plan, "TEXPORT", false, false)
			require.Error(t, err)
			rows, err := st.QueryReadOnly(context.Background(), "select ts from messages where channel_id='C1'")
			require.NoError(t, err)
			require.Len(t, rows, 500)
			for _, row := range rows {
				require.NotEqual(t, "501.000001", row["ts"])
				require.NotEqual(t, "502.000001", row["ts"])
			}
			require.NoError(t, os.WriteFile(later, []byte(files["room/2.json"]), 0600))
			retry, err := ex.Prepare("TEXPORT", admission.Exclude)
			require.NoError(t, err)
			require.NoError(t, validateImportMessageKeys(context.Background(), st, retry, "TEXPORT"))
			report, _, err := runImportExecution(context.Background(), st, retry, "TEXPORT", false, false)
			require.NoError(t, err)
			require.Equal(t, 2, report.Messages)
			require.Equal(t, 500, report.Skipped)
			rows, err = st.QueryReadOnly(context.Background(), "select ts from messages where channel_id='C1'")
			require.NoError(t, err)
			require.Len(t, rows, 502)
			keys := map[string]bool{}
			for _, row := range rows {
				keys[row["ts"].(string)] = true
			}
			require.True(t, keys["501.000001"])
			require.True(t, keys["502.000001"])
			require.NoError(t, st.Close())
		})
	}
}

func TestImportDryRunLeavesOlderSchemaUnrepaired(t *testing.T) {
	cfg, configPath := importAdmissionConfig(t, "false")
	seedImportArchive(t, cfg.DBPath)
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	_, err = st.DB().Exec("drop table messages")
	require.NoError(t, err)
	_, err = st.DB().Exec("pragma user_version = 1")
	require.NoError(t, err)
	source := importAdmissionExport(t, "zip", importAdmissionFiles())
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output}
	err = app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT", "--dry-run"})
	require.Error(t, err)
	var version int
	require.NoError(t, st.DB().QueryRow("pragma user_version").Scan(&version))
	require.Equal(t, 1, version)
	var messages int
	require.NoError(t, st.DB().QueryRow("select count(*) from sqlite_schema where type='table' and name='messages'").Scan(&messages))
	require.Zero(t, messages)
	require.NoError(t, st.Close())
	require.False(t, importAdmissionExists(t, cfg.CacheDir))
	require.False(t, importAdmissionExists(t, cfg.LogDir))
}

func TestImportPresentEmptyCatalogAndHumanOmissions(t *testing.T) {
	cfg, configPath := importAdmissionConfig(t, "false")
	for _, files := range []map[string]string{{"channels.json": "[]"}, {"dms.json": `[{"id":"D1","name":"excluded"}]`, "excluded/1.json": "unparsed"}} {
		source := importAdmissionExport(t, "zip", files)
		var output bytes.Buffer
		app := &App{Stdout: &output, Stderr: &output}
		require.NoError(t, app.Run(context.Background(), []string{"--config", configPath, "import", source, "--workspace", "TEXPORT"}))
		if files["dms.json"] != "" {
			require.Contains(t, output.String(), "Completed with omissions: 1 DM conversations excluded.")
		}
		require.False(t, importAdmissionExists(t, cfg.DBPath))
		require.False(t, importAdmissionExists(t, cfg.CacheDir))
	}
}

func TestImportPreflightFinishesBeforeArchiveWrites(t *testing.T) {
	for _, mode := range []string{"identity-after-batch", "later-channel-collision"} {
		t.Run(mode, func(t *testing.T) {
			cfg, configPath := importAdmissionConfig(t, "false")
			seedImportArchive(t, cfg.DBPath)
			st, err := store.Open(cfg.DBPath)
			require.NoError(t, err)
			if mode == "later-channel-collision" {
				message, mentions, _ := toStoreMessage("TOTHER", "C2", map[string]any{"ts": "1", "text": "prior foreign row"}, time.Now().UTC())
				_, err = st.ApplyWriteBatch(context.Background(), store.WriteBatch{Messages: []store.MessageWrite{{Message: message, Mentions: mentions}}})
				require.NoError(t, err)
			}
			require.NoError(t, st.Close())
			before := importAdmissionSnapshot(t, cfg.DBPath)
			bodies := make([]string, 501)
			for i := range bodies {
				bodies[i] = importAdmissionMessage(fmt.Sprintf("%d.000001", i+1), "preflight-kept")
			}
			files := map[string]string{
				"channels.json": `[{"id":"C1","name":"first"},{"id":"C2","name":"later"}]`,
				"users.json":    `[{"id":"UNEW","name":"new import user"}]`,
				"later/1.json":  `[{"ts":"1","text":"later channel"}]`,
			}
			wantError := "another workspace"
			if mode == "identity-after-batch" {
				bodies = append(bodies, `{"ts":"502.000001","channel":"CFORBIDDEN","text":"late-identity-canary"}`)
				wantError = "identity"
			}
			files["first/1.json"] = "[" + strings.Join(bodies, ",") + "]"
			source := importAdmissionExport(t, "zip", files)
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			err = app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
			require.ErrorContains(t, err, wantError)
			require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
			require.NotContains(t, output.String()+err.Error(), "CFORBIDDEN")
			require.NotContains(t, output.String()+err.Error(), "late-identity-canary")
		})
	}
}

func TestImportDuplicateJSONKeys(t *testing.T) {
	for _, tc := range []struct {
		name, catalog, body string
		legacyError, clean  bool
	}{
		{name: "dm-true-false", catalog: `{"id":"C1","name":"room","is_channel":true,"is_im":true,"is_im":false}`},
		{name: "dm-false-true", catalog: `{"id":"C1","name":"room","is_channel":true,"is_im":false,"is_im":true}`},
		{name: "mixed-dm-true-false", catalog: `{"id":"C1","name":"room","is_channel":true,"is_im":true,"IS_IM":false}`},
		{name: "mixed-dm-false-true", catalog: `{"id":"C1","name":"room","is_channel":true,"IS_IM":false,"is_im":true}`},
		{name: "mixed-channel", catalog: `{"id":"C1","name":"room","is_channel":true,"IS_CHANNEL":false}`},

		{name: "escaped-key", catalog: `{"id":"C1","name":"room","is_channel":true,"is_im":true,"\u0069s_im":false}`},
		{name: "id", catalog: `{"id":"CFORBIDDEN","id":"C1","name":"room"}`},
		{name: "name", catalog: `{"id":"C1","name":"forbidden","name":"room"}`},
		{name: "folded-id", catalog: `{"id":"CFORBIDDEN","ID":"C1","name":"room"}`},
		{name: "folded-name", catalog: `{"id":"C1","name":"forbidden","NAME":"room"}`},
		{name: "folded-privacy", catalog: `{"id":"C1","name":"room","is_private":true,"IS_PRIVATE":false}`},
		{name: "unicode-folded-privacy", catalog: `{"id":"C1","name":"room","is_private":true,"iſ_private":false}`},
		{name: "catalog-latest", catalog: `{"id":"C1","name":"room","latest":{"channel":"CFORBIDDEN","channel":"C1"}}`},
		{name: "nested-foreign-admitted", body: `[{"ts":"1","text":"kept","previous":{"channel":"CFORBIDDEN","channel":"C1"}}]`},
		{name: "nested-admitted-foreign", body: `[{"ts":"1","text":"kept","previous":{"channel":"C1","channel":"CFORBIDDEN"}}]`, legacyError: true},
		{name: "over-depth", body: strings.Repeat("[", 10001) + "0" + strings.Repeat("]", 10001), legacyError: true},
		{name: "clean", body: `[{"ts":"1","text":"kept","previous":{"channel":"C1"},"blocks":[{"text":"one"},{"text":"two"}]}]`, clean: true},
	} {
		for _, policy := range []string{"omitted", "false", "true"} {
			t.Run(tc.name+"/"+policy, func(t *testing.T) {
				cfg, configPath := importAdmissionConfig(t, policy)
				seedImportArchive(t, cfg.DBPath)
				before := importAdmissionSnapshot(t, cfg.DBPath)
				catalog, body := tc.catalog, tc.body
				if catalog == "" {
					catalog = `{"id":"C1","name":"room"}`
				}
				if body == "" {
					body = `[{"ts":"1","text":"kept"}]`
				}
				source := importAdmissionExport(t, "zip", map[string]string{"channels.json": "[" + catalog + "]", "room/1.json": body})
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output}
				err := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
				if (policy == "false" && !tc.clean) || tc.legacyError {
					require.Error(t, err)
					if policy == "false" {
						require.ErrorContains(t, err, "requires unique keys")
					}
					require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
					require.NotContains(t, output.String()+err.Error(), "CFORBIDDEN")
					if policy == "false" && tc.catalog != "" {
						require.False(t, importAdmissionExists(t, cfg.CacheDir))
						require.False(t, importAdmissionExists(t, cfg.LogDir))
					}
					return
				}
				require.NoError(t, err)
				after := importAdmissionSnapshot(t, cfg.DBPath)
				ids := []string{}
				for _, row := range after["channels"] {
					ids = append(ids, row["id"].(string))
				}
				require.Contains(t, ids, "C1")
				require.Equal(t, 1, importAdmissionCount(after["messages"], "C1"))
			})
		}
	}
}

func TestImportCatalogIDCompatibility(t *testing.T) {
	for _, policy := range []string{"omitted", "false", "true"} {
		for _, whitespace := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/whitespace=%t", policy, whitespace), func(t *testing.T) {
				cfg, configPath := importAdmissionConfig(t, policy)
				seedImportArchive(t, cfg.DBPath)
				before := importAdmissionSnapshot(t, cfg.DBPath)
				catalog := `[{"id":""},{"name":"ignored"},{"id":"C1","name":"room"}]`
				id := "C1"
				if whitespace {
					catalog = `[{"id":" ","name":"room"}]`
					id = " "
				}
				source := importAdmissionExport(t, "zip", map[string]string{"channels.json": catalog, "room/1.json": `[{"ts":"1","text":"kept"}]`})
				var output bytes.Buffer
				app := &App{Stdout: &output, Stderr: &output}
				err := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
				if policy == "false" {
					require.ErrorContains(t, err, "missing identity")
					require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
					require.False(t, importAdmissionExists(t, cfg.CacheDir))
					require.False(t, importAdmissionExists(t, cfg.LogDir))
					return
				}
				require.NoError(t, err)
				var report ImportReport
				require.NoError(t, json.Unmarshal(output.Bytes(), &report))
				require.Equal(t, 1, report.Messages)
				require.Equal(t, 1, report.Channels)
				require.Equal(t, 1, importAdmissionCount(importAdmissionSnapshot(t, cfg.DBPath)["messages"], id))
			})
		}
	}
}

func TestImportCaseInsensitiveIdentityOccurrences(t *testing.T) {
	for _, policy := range []string{"omitted", "false", "true"} {
		for _, target := range []string{"channel", "message", "latest"} {
			for _, mode := range []string{"single", "foreign-first", "foreign-last", "clean"} {
				t.Run(policy+"/"+target+"/"+mode, func(t *testing.T) {
					cfg, configPath := importAdmissionConfig(t, policy)
					seedImportArchive(t, cfg.DBPath)
					before := importAdmissionSnapshot(t, cfg.DBPath)
					foreign, local := `"CFORBIDDEN"`, `"C1"`
					if mode == "clean" {
						foreign = local
					}
					if target != "channel" {
						foreign = `{"Channel":` + foreign + `}`
						local = `{"channel":` + local + `}`
					}
					members := fmt.Sprintf("%q:%s", strings.ToUpper(target), foreign)
					matching := fmt.Sprintf("%q:%s", target, local)
					if mode == "foreign-last" {
						members = matching + "," + members
					} else if mode != "single" {
						members += "," + matching
					}
					catalog, body := `{"id":"C1","name":"room"}`, `{"ts":"1","text":"kept"}`
					if target == "latest" {
						catalog = strings.TrimSuffix(catalog, "}") + "," + members + "}"
					} else {
						body = strings.TrimSuffix(body, "}") + "," + members + "}"
					}
					source := importAdmissionExport(t, "zip", map[string]string{"channels.json": "[" + catalog + "]", "room/1.json": "[" + body + "]"})
					var output bytes.Buffer
					app := &App{Stdout: &output, Stderr: &output}
					err := app.Run(context.Background(), []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"})
					if mode != "clean" {
						require.ErrorContains(t, err, "identity")
						require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
						require.NotContains(t, output.String()+err.Error(), "CFORBIDDEN")
						return
					}
					require.NoError(t, err)
					require.Equal(t, 1, importAdmissionCount(importAdmissionSnapshot(t, cfg.DBPath)["messages"], "C1"))
				})
			}
		}
	}
}

func TestImportCatalogOwnershipBeforeWrites(t *testing.T) {
	type testCase struct {
		name, policy, entity, userID string
		dry                          bool
	}
	cases := []testCase{}
	for _, policy := range []string{"omitted", "false", "true"} {
		for _, entity := range []string{"channel", "user"} {
			cases = append(cases, testCase{name: policy + "/" + entity, policy: policy, entity: entity, userID: "UCOLLISION"})
		}
	}
	for _, entity := range []string{"channel", "user"} {
		cases = append(cases, testCase{name: "dry/" + entity, policy: "false", entity: entity, userID: "UCOLLISION", dry: true})
	}
	cases = append(cases,
		testCase{name: "empty-user", policy: "false", entity: "user"},
		testCase{name: "whitespace-user", policy: "false", entity: "user", userID: " "},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, configPath := importAdmissionConfig(t, tc.policy)
			st := importCatalogStore(t, cfg.DBPath)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			if tc.entity == "channel" {
				require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: "CCOLLISION", WorkspaceID: "TFOREIGN", Name: "prior", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
			} else {
				require.NoError(t, st.UpsertUser(context.Background(), store.User{ID: tc.userID, WorkspaceID: "TFOREIGN", Name: "prior", RawJSON: "{}", UpdatedAt: now}))
			}
			require.NoError(t, st.Close())
			before := importAdmissionSnapshot(t, cfg.DBPath)
			require.Empty(t, before["messages"])
			users, err := json.Marshal([]map[string]string{{"id": "UNEW", "name": "would write first"}, {"id": tc.userID, "name": "incoming"}})
			require.NoError(t, err)
			source := importAdmissionExport(t, "zip", map[string]string{
				"channels.json": `[{"id":"CNEW","name":"new"},{"id":"CCOLLISION","name":"collision"}]`,
				"users.json":    string(users),
				"new/1.json":    `[{"ts":"1","text":"would write"}]`,
			})
			args := []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"}
			if tc.dry {
				args = append(args, "--dry-run")
			} else if tc.policy == "false" && tc.entity == "channel" {
				args = append(args, "--force")
			}
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output}
			err = app.Run(context.Background(), args)
			require.EqualError(t, err, "import catalog identity belongs to another workspace")
			require.Empty(t, output.String())
			require.Equal(t, before, importAdmissionSnapshot(t, cfg.DBPath))
			for _, identity := range []string{"CCOLLISION", "UCOLLISION", "TFOREIGN"} {
				require.NotContains(t, output.String()+err.Error(), identity)
			}
		})
	}
}

func TestImportCatalogOwnershipCompatibleIdentities(t *testing.T) {
	cfg, configPath := importAdmissionConfig(t, "false")
	st := importCatalogStore(t, cfg.DBPath)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	for _, id := range []string{"UKEEP", "", " "} {
		require.NoError(t, st.UpsertUser(ctx, store.User{ID: id, WorkspaceID: "TEXPORT", Name: "prior", RawJSON: "{}", UpdatedAt: now}))
	}
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "CKEEP", WorkspaceID: "TEXPORT", Name: "prior", Kind: "public_channel", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: "DEXCLUDED", WorkspaceID: "TFOREIGN", Name: "prior DM", Kind: "im", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, st.Close())
	before := importAdmissionSnapshot(t, cfg.DBPath)
	source := importAdmissionExport(t, "zip", map[string]string{
		"channels.json":   `[{"id":"CKEEP","name":"kept"},{"id":"CNEW","name":"new"}]`,
		"dms.json":        `[{"id":"DEXCLUDED","name":"excluded"}]`,
		"users.json":      `[{"id":"UKEEP","name":"updated"},{"id":"UNEW","name":"new"},{"id":"","name":"empty"},{"id":" ","name":"whitespace"}]`,
		"kept/1.json":     `[{"ts":"1","text":"same owner"}]`,
		"new/1.json":      `[{"ts":"1","text":"new owner"}]`,
		"excluded/1.json": "excluded body must not be decoded",
	})
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output}
	require.NoError(t, app.Run(ctx, []string{"--config", configPath, "--json", "import", source, "--workspace", "TEXPORT"}))
	var report ImportReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &report))
	require.Equal(t, 2, report.Messages)
	require.Equal(t, 2, report.Channels)
	require.Equal(t, 4, report.Users)
	require.Equal(t, 1, report.OmittedDM)
	after := importAdmissionSnapshot(t, cfg.DBPath)
	channels := map[string]map[string]any{}
	for _, row := range after["channels"] {
		channels[row["id"].(string)] = row
	}
	for _, pair := range [][2]string{{"CKEEP", "kept"}, {"CNEW", "new"}} {
		require.Contains(t, channels, pair[0])
		require.Equal(t, pair[0], channels[pair[0]]["id"])
		require.Equal(t, "TEXPORT", channels[pair[0]]["workspace_id"])
		require.Equal(t, pair[1], channels[pair[0]]["name"])
		require.Equal(t, "public_channel", channels[pair[0]]["kind"])
		require.Equal(t, 1, importAdmissionCount(after["messages"], pair[0]))
	}
	users := map[string]string{}
	for _, row := range after["users"] {
		users[row["id"].(string)] = row["name"].(string)
	}
	require.Equal(t, map[string]string{"UKEEP": "updated", "UNEW": "new", "": "empty", " ": "whitespace"}, users)
	var excluded map[string]any
	for _, prior := range before["channels"] {
		if prior["id"] == "DEXCLUDED" {
			excluded = prior
		}
	}
	require.NotNil(t, excluded)
	require.Contains(t, channels, "DEXCLUDED")
	require.Equal(t, excluded, channels["DEXCLUDED"])
}

func TestImportCatalogOwnershipLookupErrors(t *testing.T) {
	for _, entity := range []string{"channel", "user"} {
		t.Run(entity, func(t *testing.T) {
			cfg, _ := importAdmissionConfig(t, "omitted")
			st := importCatalogStore(t, cfg.DBPath)
			defer st.Close()
			files := map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`}
			if entity == "user" {
				files = map[string]string{"channels.json": "[]", "users.json": `[{"id":"U1"}]`}
			}
			ex, err := importer.Open(importAdmissionExport(t, "zip", files))
			require.NoError(t, err)
			defer ex.Close()
			plan, err := ex.Prepare("TEXPORT", admission.Default)
			require.NoError(t, err)
			require.NoError(t, validateImportMessageKeys(context.Background(), st, plan, "TEXPORT"))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _, err = runImportExecution(ctx, st, plan, "TEXPORT", true, false)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func importCatalogStore(t *testing.T, dbPath string) *store.Store {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(dbPath), 0700))
	st, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	for _, id := range []string{"TEXPORT", "TFOREIGN"} {
		require.NoError(t, st.UpsertWorkspace(context.Background(), store.Workspace{ID: id, Name: "preserved metadata", RawJSON: `{"prior":true}`, UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
	}
	return st
}
