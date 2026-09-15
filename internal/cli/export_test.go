package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const exportTestRevision = "0123456789abcdef0123456789abcdef01234567"
const exportTestSelection = `{"workspace_id":"T1","workspace_label":"workspace-label-canary","channels":[{"channel_id":"C1","label":"channel-label-canary"}],"messages":[{"channel_id":"C1","ts":"1710000000.000100","text":{"mode":"replace","replacement":"replacement-canary"}}]}`

func exportTestBuildInfo(revision string) (*debug.BuildInfo, bool) {
	return &debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: revision}, {Key: "vcs.modified", Value: "false"},
	}}, true
}

func exportCLIFixture(t *testing.T) (string, string, *App, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	db := filepath.Join(dir, "archive.db")
	seedArchiveStore(t, db, "source-text-canary")
	s, err := store.Open(db)
	require.NoError(t, err)
	_, err = s.DB().Exec(`update channels set raw_json = '{"id":"C1","is_channel":true,"is_private":false,"is_group":false,"is_im":false,"is_mpim":false,"private":"raw-canary"}'`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	selection := filepath.Join(dir, "selection.json")
	require.NoError(t, os.WriteFile(selection, []byte(exportTestSelection+"\n"), 0o600))
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: &output, readBuildInfo: func() (*debug.BuildInfo, bool) { return exportTestBuildInfo(exportTestRevision) }}
	return db, selection, app, &output
}

func TestExportCLIWorkflow(t *testing.T) {
	for _, format := range []string{"json", "text", "log"} {
		t.Run(format, func(t *testing.T) {
			db, selection, app, output := exportCLIFixture(t)
			before := importAdmissionSnapshot(t, db)
			archive, err := os.ReadFile(db)
			require.NoError(t, err)
			t.Setenv("HOME", "")
			t.Setenv("SLACRAWL_NO_UPDATE_CHECK", "")
			t.Setenv("PATH", t.TempDir()) // Any accidental Git/child process cannot run.
			plan := filepath.Join(filepath.Dir(db), "plan.json")
			dest := filepath.Join(filepath.Dir(db), "projection")
			app.configPath = "retained-config-canary"
			prefix := []string{"--config", selection, "--format", format, "export"}
			require.NoError(t, app.Run(context.Background(), append(prefix, "prepare", "--db", db, "--selection", selection, "--out", plan)))
			body, err := os.ReadFile(plan)
			require.NoError(t, err)
			var envelope exportPrivatePlan
			require.NoError(t, json.Unmarshal(body, &envelope))
			require.Equal(t, 1, envelope.Version)
			require.Equal(t, exportTestRevision, envelope.ProducerRevision)
			canonical, err := json.Marshal(envelope)
			require.NoError(t, err)
			require.Equal(t, append(canonical, '\n'), body)
			info, err := os.Stat(plan)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			require.NotContains(t, output.String(), "canary")
			output.Reset()
			require.NoError(t, app.Run(context.Background(), append(prefix, "build", "--db", db, "--plan", plan, "--out", dest)))
			buildOutput := output.String()
			if format == "json" {
				var receipt map[string]any
				require.NoError(t, json.Unmarshal(output.Bytes(), &receipt))
				require.Len(t, receipt, 5)
				for _, field := range []string{"manifest_sha256", "messages_sha256", "manifest_bytes", "messages_bytes", "rows"} {
					require.Contains(t, receipt, field)
				}
				require.Equal(t, float64(1), receipt["rows"])
			}
			output.Reset()
			require.NoError(t, app.Run(context.Background(), append(prefix, "verify", "--db", db, "--plan", plan, "--dir", dest)))
			require.Equal(t, buildOutput, output.String())
			require.NotContains(t, output.String(), "canary")
			messages, err := os.ReadFile(filepath.Join(dest, "messages.jsonl"))
			require.NoError(t, err)
			require.Equal(t, "{\"channel_id\":\"C1\",\"ts\":\"1710000000.000100\",\"user_id\":\"U1\",\"text\":\"replacement-canary\",\"thread_ts\":\"\",\"edited_ts\":\"\"}\n", string(messages))
			require.NotContains(t, string(messages), "source-text-canary")
			require.NotContains(t, string(messages), "raw-canary")
			after, err := os.ReadFile(db)
			require.NoError(t, err)
			require.Equal(t, archive, after)
			require.Equal(t, before, importAdmissionSnapshot(t, db))
			require.Equal(t, "retained-config-canary", app.configPath)
		})
	}
}

func TestExportCLIProducerAndVerifierRevisions(t *testing.T) {
	db, selection, app, output := exportCLIFixture(t)
	plan, dest := filepath.Join(filepath.Dir(db), "plan"), filepath.Join(filepath.Dir(db), "artifact")
	require.NoError(t, app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", plan}))
	require.NoError(t, app.Run(context.Background(), []string{"export", "build", "--db", db, "--plan", plan, "--out", dest}))
	for _, verifier := range []string{"newer", "unstamped", "dirty"} {
		t.Run(verifier, func(t *testing.T) {
			app.readBuildInfo = func() (*debug.BuildInfo, bool) {
				if verifier == "unstamped" {
					return nil, false
				}
				info, ok := exportTestBuildInfo(strings.Repeat("b", 40))
				if verifier == "dirty" {
					info.Settings[2].Value = "true"
				}
				return info, ok
			}
			output.Reset()
			app.configPath = "retained-config-canary"
			require.NoError(t, app.Run(context.Background(), []string{"export", "verify", "--db", db, "--plan", plan, "--dir", dest}))
			output.Reset()
			other := filepath.Join(filepath.Dir(db), verifier)
			require.Error(t, app.Run(context.Background(), []string{"export", "build", "--db", db, "--plan", plan, "--out", other}))
			require.NoDirExists(t, other)
			require.Empty(t, output.String())
		})
	}
}

func TestExportBuildIdentity(t *testing.T) {
	for _, mode := range []string{"valid", "unavailable", "nil", "missing-vcs", "missing-revision", "missing-modified", "dirty", "other-vcs", "short", "uppercase", "nonhex", "duplicate-vcs", "duplicate-revision", "duplicate-modified"} {
		t.Run(mode, func(t *testing.T) {
			info, ok := exportTestBuildInfo(exportTestRevision)
			switch mode {
			case "unavailable":
				ok = false
			case "nil":
				info = nil
			case "missing-vcs":
				info.Settings = info.Settings[1:]
			case "missing-revision":
				info.Settings = append(info.Settings[:1], info.Settings[2:]...)
			case "missing-modified":
				info.Settings = info.Settings[:2]
			case "dirty":
				info.Settings[2].Value = "true"
			case "other-vcs":
				info.Settings[0].Value = "hg"
			case "short":
				info.Settings[1].Value = "abcdef"
			case "uppercase":
				info.Settings[1].Value = strings.ToUpper(exportTestRevision)
			case "nonhex":
				info.Settings[1].Value = strings.Repeat("x", 40)
			case "duplicate-vcs":
				info.Settings = append(info.Settings, info.Settings[0])
			case "duplicate-revision":
				info.Settings = append(info.Settings, info.Settings[1])
			case "duplicate-modified":
				info.Settings = append(info.Settings, info.Settings[2])
			}
			revision, err := cleanExportRevision(info, ok)
			if mode == "valid" {
				require.NoError(t, err)
				require.Equal(t, exportTestRevision, revision)
			} else {
				require.ErrorContains(t, err, "go build -buildvcs=true")
				require.Empty(t, revision)
			}
		})
	}
}

func TestExportCLIRejectsChangedSource(t *testing.T) {
	for _, command := range []string{"build", "verify"} {
		t.Run(command, func(t *testing.T) {
			db, selection, app, output := exportCLIFixture(t)
			plan, dest := filepath.Join(filepath.Dir(db), "plan"), filepath.Join(filepath.Dir(db), "artifact")
			require.NoError(t, app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", plan}))
			if command == "verify" {
				require.NoError(t, app.Run(context.Background(), []string{"export", "build", "--db", db, "--plan", plan, "--out", dest}))
			}
			before := sharePublishTree(t, dest)
			s, err := store.Open(db)
			require.NoError(t, err)
			_, err = s.DB().Exec(`update messages set text = 'changed-source-canary'`)
			require.NoError(t, err)
			require.NoError(t, s.Close())
			output.Reset()
			flag := "--out"
			if command == "verify" {
				flag = "--dir"
			}
			err = app.Run(context.Background(), []string{"export", command, "--db", db, "--plan", plan, flag, dest})
			require.ErrorContains(t, err, "binding changed")
			require.NotContains(t, err.Error(), "canary")
			require.Empty(t, output.String())
			require.Equal(t, before, sharePublishTree(t, dest))
		})
	}
}

func TestExportCLISelectionEligibility(t *testing.T) {
	for _, mode := range []string{"dm", "mpim", "private", "draft", "deleted", "missing-parent"} {
		t.Run(mode, func(t *testing.T) {
			db, selection, app, output := exportCLIFixture(t)
			s, err := store.Open(db)
			require.NoError(t, err)
			sql := map[string]string{
				"dm":             `update channels set kind = 'im', is_private = 1`,
				"mpim":           `update channels set kind = 'mpim', is_private = 1`,
				"private":        `update channels set kind = 'private_channel', is_private = 1`,
				"draft":          `update messages set subtype = 'desktop_draft'`,
				"deleted":        `update messages set deleted_ts = '1710000001.000100'`,
				"missing-parent": `update messages set thread_ts = '1709999999.000100'`,
			}[mode]
			_, err = s.DB().Exec(sql)
			require.NoError(t, err)
			require.NoError(t, s.Close())
			before := importAdmissionSnapshot(t, db)
			dest := filepath.Join(filepath.Dir(db), "plan")
			err = app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", dest})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "canary")
			require.Empty(t, output.String())
			require.NoFileExists(t, dest)
			require.Equal(t, before, importAdmissionSnapshot(t, db))
		})
	}
}

func TestExportCLICancellation(t *testing.T) {
	for _, command := range []string{"prepare", "build", "verify"} {
		t.Run(command, func(t *testing.T) {
			db, selection, app, output := exportCLIFixture(t)
			plan := filepath.Join(filepath.Dir(db), "plan")
			require.NoError(t, app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", plan}))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			dest := filepath.Join(filepath.Dir(db), "cancelled")
			inputFlag, input, outputFlag := "--plan", plan, "--out"
			if command == "prepare" {
				inputFlag, input = "--selection", selection
			}
			if command == "verify" {
				outputFlag = "--dir"
			}
			output.Reset()
			err := app.Run(ctx, []string{"export", command, "--db", db, inputFlag, input, outputFlag, dest})
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, output.String())
			require.NoFileExists(t, dest)
			require.NoDirExists(t, dest)
		})
	}
}

func TestExportCLIInputCanonicalProfile(t *testing.T) {
	for _, kind := range []string{"selection", "plan"} {
		for _, mutation := range []string{"valid", "whitespace", "duplicate", "case", "escaped-key", "unknown", "missing", "null-array", "trailing", "bad-json"} {
			t.Run(kind+"/"+mutation, func(t *testing.T) {
				db, selection, app, output := exportCLIFixture(t)
				input := selection
				command, inputFlag := "prepare", "--selection"
				key := "workspace_id"
				if kind == "plan" {
					input = filepath.Join(filepath.Dir(db), "plan")
					require.NoError(t, app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", input}))
					command, inputFlag, key = "build", "--plan", "version"
				}
				body, err := os.ReadFile(input)
				require.NoError(t, err)
				text := string(body)
				switch mutation {
				case "whitespace":
					text = " \n" + text
				case "duplicate":
					if kind == "selection" {
						text = strings.Replace(text, `"workspace_id":"T1"`, `"workspace_id":"T1","workspace_id":"T1"`, 1)
					} else {
						text = strings.Replace(text, `"version":1`, `"version":1,"version":1`, 1)
					}
				case "case":
					text = strings.Replace(text, `"`+key+`"`, `"`+strings.ToUpper(key)+`"`, 1)
				case "escaped-key":
					text = strings.Replace(text, `"`+key+`"`, `"\u00`+map[string]string{"selection": "77orkspace_id", "plan": "76ersion"}[kind]+`"`, 1)
				case "unknown":
					text = strings.Replace(text, "{", `{"private-canary":"hidden",`, 1)
				case "missing":
					text = strings.Replace(text, `"workspace_label":"workspace-label-canary",`, "", 1)
				case "null-array":
					var envelope exportPrivatePlan
					if kind == "plan" {
						require.NoError(t, json.Unmarshal(body, &envelope))
						envelope.Selection.Channels = nil
						encoded, err := json.Marshal(envelope)
						require.NoError(t, err)
						text = string(encoded) + "\n"
					} else {
						text = strings.Replace(text, `[{"channel_id":"C1","label":"channel-label-canary"}]`, "null", 1)
					}
				case "trailing":
					text += "{}"
				case "bad-json":
					text = `{private-canary`
				}
				require.NoError(t, os.WriteFile(input, []byte(text), 0o600))
				dest := filepath.Join(filepath.Dir(db), "output")
				output.Reset()
				err = app.Run(context.Background(), []string{"export", command, "--db", db, inputFlag, input, "--out", dest})
				if mutation == "valid" || kind == "selection" && mutation == "whitespace" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.NotContains(t, err.Error(), "canary")
					require.NoFileExists(t, dest)
					require.NoDirExists(t, dest)
					require.Empty(t, output.String())
				}
			})
		}
	}
}

func TestExportCLIFileBoundaries(t *testing.T) {
	for _, mode := range []string{"input-symlink", "input-directory", "input-missing", "output-file", "output-symlink", "output-directory", "output-parent-missing", "db-missing", "db-uri"} {
		t.Run(mode, func(t *testing.T) {
			db, selection, app, output := exportCLIFixture(t)
			dir := filepath.Dir(db)
			dest := filepath.Join(dir, "output")
			switch mode {
			case "input-symlink":
				link := filepath.Join(dir, "link")
				require.NoError(t, os.Symlink(selection, link))
				selection = link
			case "input-directory":
				selection = dir
			case "input-missing":
				selection = filepath.Join(dir, "missing")
			case "output-file":
				require.NoError(t, os.WriteFile(dest, []byte("existing-canary"), 0o600))
			case "output-symlink":
				require.NoError(t, os.Symlink(selection, dest))
			case "output-directory":
				require.NoError(t, os.Mkdir(dest, 0o700))
			case "output-parent-missing":
				dest = filepath.Join(dir, "absent", "output")
			case "db-missing":
				db = filepath.Join(dir, "missing.db")
			case "db-uri":
				db = "file:" + filepath.Join(dir, "missing.db") + "?mode=rwc"
			}
			selectionBefore, _ := os.ReadFile(selection)
			outputBefore, _ := os.ReadFile(dest)
			err := app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", dest})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "canary")
			require.Empty(t, output.String())
			selectionAfter, _ := os.ReadFile(selection)
			outputAfter, _ := os.ReadFile(dest)
			require.Equal(t, selectionBefore, selectionAfter)
			require.Equal(t, outputBefore, outputAfter)
			require.NoFileExists(t, filepath.Join(dir, "missing.db"))
		})
	}
}

func TestExportCLIBuildCollision(t *testing.T) {
	db, selection, app, output := exportCLIFixture(t)
	plan, dest := filepath.Join(filepath.Dir(db), "plan"), filepath.Join(filepath.Dir(db), "artifact")
	require.NoError(t, app.Run(context.Background(), []string{"export", "prepare", "--db", db, "--selection", selection, "--out", plan}))
	args := []string{"export", "build", "--db", db, "--plan", plan, "--out", dest}
	require.NoError(t, app.Run(context.Background(), args))
	before := sharePublishTree(t, dest)
	output.Reset()
	require.Error(t, app.Run(context.Background(), args))
	require.Empty(t, output.String())
	require.Equal(t, before, sharePublishTree(t, dest))
}

type exportFailWriter struct{}

func (exportFailWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestExportCLIHelpAndOutputFailures(t *testing.T) {
	for _, command := range []string{"help", "prepare", "build", "verify"} {
		t.Run(command, func(t *testing.T) {
			t.Setenv("HOME", "")
			t.Setenv("SLACRAWL_NO_UPDATE_CHECK", "")
			var output bytes.Buffer
			app := &App{Stdout: &output, Stderr: &output, readBuildInfo: func() (*debug.BuildInfo, bool) { t.Fatal("help must not inspect build identity"); return nil, false }}
			args := []string{"export", command}
			if command != "help" {
				args = append(args, "--help")
			}
			require.NoError(t, app.Run(context.Background(), args))
			require.Contains(t, output.String(), "Usage")
			app.Stdout = exportFailWriter{}
			require.EqualError(t, app.Run(context.Background(), args), "output unavailable")
		})
	}
	for _, command := range []string{"prepare", "build", "verify"} {
		t.Run(command+"-receipt", func(t *testing.T) {
			db, selection, app, _ := exportCLIFixture(t)
			plan, dest := filepath.Join(filepath.Dir(db), "plan"), filepath.Join(filepath.Dir(db), "artifact")
			prepare := []string{"export", "prepare", "--db", db, "--selection", selection, "--out", plan}
			build := []string{"export", "build", "--db", db, "--plan", plan, "--out", dest}
			args := prepare
			if command != "prepare" {
				require.NoError(t, app.Run(context.Background(), prepare))
				args = build
			}
			if command == "verify" {
				require.NoError(t, app.Run(context.Background(), build))
				args = []string{"export", "verify", "--db", db, "--plan", plan, "--dir", dest}
			}
			app.Stdout = exportFailWriter{}
			require.ErrorContains(t, app.Run(context.Background(), args), "output unavailable")
			require.FileExists(t, plan)
		})
	}
}

func TestExportCLIRejectsAmbientAndMisplacedFlags(t *testing.T) {
	t.Setenv("HOME", "")
	for _, args := range [][]string{
		{"export", "--config", "private-config-canary"}, {"export", "--json", "prepare"},
		{"export", "prepare"}, {"export", "build", "--db", "archive"}, {"export", "verify", "--out", "wrong"},
	} {
		var output bytes.Buffer
		app := &App{Stdout: &output, Stderr: io.Discard}
		require.Error(t, app.Run(context.Background(), args))
		require.Empty(t, output.String())
	}
	var output bytes.Buffer
	app := &App{Stdout: &output, Stderr: io.Discard, configPath: "retained-config-canary"}
	require.NoError(t, app.Run(context.Background(), []string{"--config", "missing-config-canary", "export", "help"}))
	require.Contains(t, output.String(), "Usage")
	require.Equal(t, "retained-config-canary", app.configPath)
}
