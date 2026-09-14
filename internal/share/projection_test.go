package share

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const projectionTestRevision = "0123456789abcdef0123456789abcdef01234567"

const projectionTestMessages = "{\"channel_id\":\"C1\",\"ts\":\"1\",\"user_id\":\"U2\",\"text\":\"kept text\",\"thread_ts\":null,\"edited_ts\":\"\"}\n" +
	"{\"channel_id\":\"C1\",\"ts\":\"2\",\"user_id\":\"U1\",\"text\":\"reviewed text\",\"thread_ts\":\"1\",\"edited_ts\":null}\n"

func projectionTestExpected() ExportProjection {
	return ExportProjection{
		WorkspaceID: "T1", WorkspaceLabel: "Workspace", Channels: []ExportChannelSelection{{ChannelID: "C1", Label: "Channel"}},
		Messages: []ExportMessage{
			{ChannelID: "C1", TS: "1", UserID: new("U2"), Text: "kept text", EditedTS: new("")},
			{ChannelID: "C1", TS: "2", UserID: new("U1"), Text: "reviewed text", ThreadTS: new("1")},
		},
	}
}

// The oracle is a literal profile, independent of the production encoders.
func projectionTestManifest(messages string) string {
	return fmt.Sprintf("{\"format\":\"slacrawl-projection\",\"version\":1,\"producer_revision\":\"%s\",\"workspace_id\":\"T1\",\"workspace_label\":\"Workspace\",\"channels\":[{\"channel_id\":\"C1\",\"label\":\"Channel\"}],\"author_ids\":[\"U1\",\"U2\"],\"payload\":{\"path\":\"messages.jsonl\",\"sha256\":\"%x\",\"bytes\":%d,\"rows\":2}}\n", projectionTestRevision, sha256.Sum256([]byte(messages)), len(messages))
}

func writeManualProjection(t *testing.T, dir, messages, manifest string) {
	t.Helper()
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "messages.jsonl"), []byte(messages), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o600))
}

func TestProjectionWriteAndIndependentVerify(t *testing.T) {
	ctx := context.Background()
	expected := projectionTestExpected()
	manifest := projectionTestManifest(projectionTestMessages)
	var previous ProjectionReceipt
	for _, name := range []string{"first", "second", "manual"} {
		dir := filepath.Join(t.TempDir(), name)
		var receipt ProjectionReceipt
		var err error
		if name == "manual" {
			writeManualProjection(t, dir, projectionTestMessages, manifest)
			receipt, err = VerifyProjection(ctx, dir, expected, projectionTestRevision)
		} else {
			receipt, err = WriteProjection(ctx, dir, expected, projectionTestRevision)
		}
		require.NoError(t, err)
		require.Equal(t, ProjectionReceipt{
			ManifestSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(manifest))), MessagesSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(projectionTestMessages))),
			ManifestBytes: int64(len(manifest)), MessagesBytes: int64(len(projectionTestMessages)), Rows: 2,
		}, receipt)
		if name != "first" {
			require.Equal(t, previous, receipt)
		}
		previous = receipt
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
		for _, file := range []struct{ name, body string }{{"manifest.json", manifest}, {"messages.jsonl", projectionTestMessages}} {
			body, err := os.ReadFile(filepath.Join(dir, file.name))
			require.NoError(t, err)
			require.Equal(t, file.body, string(body))
			info, err := os.Stat(filepath.Join(dir, file.name))
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		}
		verified, err := VerifyProjection(ctx, dir, expected, projectionTestRevision)
		require.NoError(t, err)
		require.Equal(t, receipt, verified)
	}
}

func TestProjectionKeepsOnlySelectedFields(t *testing.T) {
	_, path, selection := selectionFixture(t)
	ctx := context.Background()
	plan, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	expected, err := ResolveExportSelection(ctx, path, plan)
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "projection")
	receipt, err := WriteProjection(ctx, dir, expected, projectionTestRevision)
	require.NoError(t, err)
	require.Equal(t, 3, receipt.Rows)
	body, err := os.ReadFile(filepath.Join(dir, "messages.jsonl"))
	require.NoError(t, err)
	require.Equal(t, "{\"channel_id\":\"C1\",\"ts\":\"123.456\",\"user_id\":\"U1\",\"text\":\"selected root\",\"thread_ts\":\"123.456\",\"edited_ts\":\"125.000\"}\n"+
		"{\"channel_id\":\"C1\",\"ts\":\"124.000\",\"user_id\":\"U1\",\"text\":\"reviewed reply\",\"thread_ts\":\"123.456\",\"edited_ts\":\"\"}\n"+
		"{\"channel_id\":\"C1\",\"ts\":\"126.000\",\"user_id\":\"U1\",\"text\":\"\",\"thread_ts\":\"\",\"edited_ts\":\"\"}\n", string(body))
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	for _, private := range []string{"canary", "source_sha256", "workspace_sha256", "raw_json", "normalized_text", "subtype", "source_name", "deleted_ts", "updated_at"} {
		require.NotContains(t, string(body)+string(manifest), private)
	}
}

func TestProjectionNullableAuthors(t *testing.T) {
	expected := projectionTestExpected()
	expected.Messages[0].UserID, expected.Messages[1].UserID = nil, new("")
	dir := filepath.Join(t.TempDir(), "projection")
	_, err := WriteProjection(context.Background(), dir, expected, projectionTestRevision)
	require.NoError(t, err)
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	require.Contains(t, string(manifest), `"author_ids":[]`)
	body, err := os.ReadFile(filepath.Join(dir, "messages.jsonl"))
	require.NoError(t, err)
	require.Contains(t, string(body), `"user_id":null`)
	require.Contains(t, string(body), `"user_id":""`)
	bad := strings.Replace(string(manifest), `"author_ids":[]`, `"author_ids":null`, 1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(bad), 0o600))
	receipt, err := VerifyProjection(context.Background(), dir, expected, projectionTestRevision)
	require.ErrorContains(t, err, "not canonical")
	require.Empty(t, receipt)
}

func TestProjectionFreshDestination(t *testing.T) {
	for _, mode := range []string{"directory", "file", "symlink", "missing-parent", "empty"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "destination")
			target := filepath.Join(parent, "target")
			switch mode {
			case "directory":
				require.NoError(t, os.Mkdir(dir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "retained"), []byte("destination-canary"), 0o600))
			case "file":
				require.NoError(t, os.WriteFile(dir, []byte("destination-canary"), 0o600))
			case "symlink":
				require.NoError(t, os.Mkdir(target, 0o700))
				require.NoError(t, os.Symlink(target, dir))
			case "missing-parent":
				dir = filepath.Join(parent, "absent", "child")
			case "empty":
				dir = ""
			}
			receipt, err := WriteProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
			if mode == "directory" || mode == "file" {
				path := dir
				if mode == "directory" {
					path = filepath.Join(dir, "retained")
				}
				body, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, "destination-canary", string(body))
			}
			if mode == "symlink" {
				entries, err := os.ReadDir(target)
				require.NoError(t, err)
				require.Empty(t, entries)
			}
			if mode == "missing-parent" {
				require.NoDirExists(t, filepath.Join(parent, "absent"))
			}
		})
	}

	t.Run("race", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "destination")
		start := make(chan struct{})
		var wg sync.WaitGroup
		var receipts [2]ProjectionReceipt
		var errs [2]error
		for i := range 2 {
			wg.Go(func() {
				<-start
				receipts[i], errs[i] = WriteProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			})
		}
		close(start)
		wg.Wait()
		passed := 0
		for i, err := range errs {
			if err == nil {
				passed++
				require.Equal(t, 2, receipts[i].Rows)
			} else {
				require.Empty(t, receipts[i])
			}
		}
		require.Equal(t, 1, passed)
		_, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
		require.NoError(t, err)
	})
}

func TestProjectionRejectsInvalidExpected(t *testing.T) {
	for _, mode := range []string{"nil-channels", "nil-messages", "empty-workspace", "channel-order", "duplicate-channel", "message-order", "duplicate-message", "unknown-channel", "empty-ts", "invalid-text", "invalid-author", "missing-parent", "parent-chain", "parent-cycle", "revision-short", "revision-uppercase"} {
		t.Run(mode, func(t *testing.T) {
			expected := projectionTestExpected()
			revision := projectionTestRevision
			switch mode {
			case "nil-channels":
				expected.Channels = nil
			case "nil-messages":
				expected.Messages = nil
			case "empty-workspace":
				expected.WorkspaceID = ""
			case "channel-order":
				expected.Channels = append(expected.Channels, ExportChannelSelection{ChannelID: "C0", Label: "label-canary"})
			case "duplicate-channel":
				expected.Channels = append(expected.Channels, expected.Channels[0])
			case "message-order":
				slices.Reverse(expected.Messages)
			case "duplicate-message":
				expected.Messages[1] = expected.Messages[0]
			case "unknown-channel":
				expected.Messages[1].ChannelID = "channel-canary"
			case "empty-ts":
				expected.Messages[1].TS = ""
			case "invalid-text":
				expected.Messages[1].Text = "text-canary\xff"
			case "invalid-author":
				expected.Messages[1].UserID = new("author-canary\xff")
			case "missing-parent":
				expected.Messages[1].ThreadTS = new("parent-canary")
			case "parent-chain":
				expected.Messages[0].ThreadTS = new("0")
				expected.Messages = append([]ExportMessage{{ChannelID: "C1", TS: "0", Text: "root"}}, expected.Messages...)
			case "parent-cycle":
				expected.Messages[0].ThreadTS = new("2")
			case "revision-short":
				revision = "revision-canary"
			case "revision-uppercase":
				revision = strings.ToUpper(revision)
			}
			dir := filepath.Join(t.TempDir(), "projection")
			receipt, err := WriteProjection(context.Background(), dir, expected, revision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NoDirExists(t, dir)
			require.NotContains(t, err.Error(), "canary")
			receipt, err = VerifyProjection(context.Background(), dir, expected, revision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

func TestProjectionRejectsChangedMessageBytes(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"unknown", `"text":"kept text"`, `"text":"kept text","raw_json":"raw-canary"`},
		{"duplicate", `"text":"kept text"`, `"text":"kept text","text":"kept text"`},
		{"casefold", `"text":"kept text"`, `"text":"kept text","TEXT":"kept text"`},
		{"escaped-key", `"text":"kept text"`, `"te\u0078t":"kept text"`},
		{"missing-field", `"user_id":"U2",`, ""},
		{"missing-last-field", `,"edited_ts":""`, ""},
		{"reordered-fields", `"channel_id":"C1","ts":"1"`, `"ts":"1","channel_id":"C1"`},
		{"leading-space", `{"channel_id"`, ` {"channel_id"`},
		{"null-text", `"text":"kept text"`, `"text":null`},
		{"wrong-type", `"text":"kept text"`, `"text":17`},
		{"changed-text", `"text":"kept text"`, `"text":"changed-canary"`},
		{"changed-user", `"user_id":"U2"`, `"user_id":"user-canary"`},
		{"null-empty", `"edited_ts":""`, `"edited_ts":null`},
		{"changed-thread", `"thread_ts":"1"`, `"thread_ts":"parent-canary"`},
		{"unknown-channel", `"channel_id":"C1"`, `"channel_id":"channel-canary"`},
		{"invalid-utf8", `"text":"kept text"`, "\"text\":\"bad\xff\""},
		{"surrogate", `"text":"kept text"`, `"text":"\ud800"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messages := strings.Replace(projectionTestMessages, tc.from, tc.to, 1)
			require.NotEqual(t, projectionTestMessages, messages)
			dir := filepath.Join(t.TempDir(), "projection")
			// Update the digest too: verification must independently enforce fields.
			writeManualProjection(t, dir, messages, projectionTestManifest(messages))
			receipt, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
		})
	}
	for _, mode := range []string{"no-lf", "crlf", "blank-line", "trailing-data", "extra-row", "missing-row", "row-order", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			lines := strings.SplitAfter(projectionTestMessages, "\n")
			messages := projectionTestMessages
			switch mode {
			case "no-lf":
				messages = strings.TrimSuffix(messages, "\n")
			case "crlf":
				messages = strings.ReplaceAll(messages, "\n", "\r\n")
			case "blank-line":
				messages += "\n"
			case "trailing-data":
				messages += "payload-canary"
			case "extra-row":
				messages += lines[0]
			case "missing-row":
				messages = lines[0]
			case "row-order":
				messages = lines[1] + lines[0]
			case "oversize":
				messages = strings.Repeat("x", 16384) + "\n"
			}
			dir := filepath.Join(t.TempDir(), "projection")
			writeManualProjection(t, dir, messages, projectionTestManifest(messages))
			receipt, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

func TestProjectionRejectsChangedManifest(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"unknown", `"version":1`, `"version":1,"source_sha256":"binding-canary"`},
		{"duplicate", `"version":1`, `"version":1,"version":1`},
		{"casefold", `"version":1`, `"VERSION":1`},
		{"escaped-duplicate", `"version":1`, `"version":1,"vers\u0069on":1`},
		{"missing", `"version":1,`, ""},
		{"null-channels", `"channels":[{"channel_id":"C1","label":"Channel"}]`, `"channels":null`},
		{"null-authors", `"author_ids":["U1","U2"]`, `"author_ids":null`},
		{"null-payload", `"payload":{`, `"payload":null,"ignored":{`},
		{"wrong-format", `"format":"slacrawl-projection"`, `"format":"other"`},
		{"wrong-version", `"version":1`, `"version":2`},
		{"revision", projectionTestRevision, strings.Repeat("f", 40)},
		{"workspace", `"workspace_id":"T1"`, `"workspace_id":"workspace-canary"`},
		{"label", `"workspace_label":"Workspace"`, `"workspace_label":"label-canary"`},
		{"channel", `"channel_id":"C1"`, `"channel_id":"channel-canary"`},
		{"channel-label", `"label":"Channel"`, `"label":"label-canary"`},
		{"authors-order", `"author_ids":["U1","U2"]`, `"author_ids":["U2","U1"]`},
		{"authors-duplicate", `"author_ids":["U1","U2"]`, `"author_ids":["U1","U1"]`},
		{"payload-path", `"path":"messages.jsonl"`, `"path":"../messages.jsonl"`},
		{"rows", `"rows":2`, `"rows":3`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := strings.Replace(projectionTestManifest(projectionTestMessages), tc.from, tc.to, 1)
			require.NotEqual(t, projectionTestManifest(projectionTestMessages), manifest)
			dir := filepath.Join(t.TempDir(), "projection")
			writeManualProjection(t, dir, projectionTestMessages, manifest)
			receipt, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
		})
	}
	for _, mode := range []string{"no-lf", "extra-lf", "trailing-value", "checksum", "bytes", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			manifest := projectionTestManifest(projectionTestMessages)
			switch mode {
			case "no-lf":
				manifest = strings.TrimSuffix(manifest, "\n")
			case "extra-lf":
				manifest += "\n"
			case "trailing-value":
				manifest += "{}\n"
			case "checksum":
				manifest = strings.Replace(manifest, fmt.Sprintf("%x", sha256.Sum256([]byte(projectionTestMessages))), strings.Repeat("0", 64), 1)
			case "bytes":
				manifest = strings.Replace(manifest, fmt.Sprintf(`"bytes":%d`, len(projectionTestMessages)), `"bytes":0`, 1)
			case "oversize":
				manifest += strings.Repeat(" ", 16384)
			}
			dir := filepath.Join(t.TempDir(), "projection")
			writeManualProjection(t, dir, projectionTestMessages, manifest)
			receipt, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
		})
	}
}

func TestProjectionRejectsUnexpectedFilesystem(t *testing.T) {
	for _, mode := range []string{"extra", "missing", "directory", "file-symlink", "contained-symlink", "hardlink", "root-symlink", "root-symlink-slash", "root-symlink-dot", "empty-root"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "projection")
			writeManualProjection(t, dir, projectionTestMessages, projectionTestManifest(projectionTestMessages))
			switch mode {
			case "extra":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "unexpected-canary"), nil, 0o600))
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(dir, "messages.jsonl")))
			case "directory":
				require.NoError(t, os.Remove(filepath.Join(dir, "messages.jsonl")))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "messages.jsonl"), 0o700))
			case "file-symlink":
				outside := filepath.Join(parent, "outside")
				require.NoError(t, os.WriteFile(outside, []byte(projectionTestMessages), 0o600))
				require.NoError(t, os.Remove(filepath.Join(dir, "messages.jsonl")))
				require.NoError(t, os.Symlink(outside, filepath.Join(dir, "messages.jsonl")))
			case "contained-symlink":
				require.NoError(t, os.Remove(filepath.Join(dir, "messages.jsonl")))
				require.NoError(t, os.Symlink("manifest.json", filepath.Join(dir, "messages.jsonl")))
			case "hardlink":
				require.NoError(t, os.Remove(filepath.Join(dir, "manifest.json")))
				require.NoError(t, os.Link(filepath.Join(dir, "messages.jsonl"), filepath.Join(dir, "manifest.json")))
			case "root-symlink", "root-symlink-slash", "root-symlink-dot":
				link := filepath.Join(parent, "link")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
				if mode == "root-symlink-slash" {
					dir += "/"
				}
				if mode == "root-symlink-dot" {
					dir += "/."
				}
			case "empty-root":
				dir = ""
			}
			receipt, err := VerifyProjection(context.Background(), dir, projectionTestExpected(), projectionTestRevision)
			require.Error(t, err)
			require.Empty(t, receipt)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

func TestProjectionRechecksOpenedFileIdentity(t *testing.T) {
	for _, mode := range []string{"replacement", "symlink", "size", "closed"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "projection")
			writeManualProjection(t, dir, projectionTestMessages, projectionTestManifest(projectionTestMessages))
			root, err := os.OpenRoot(dir)
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			file, info, err := openProjectionFile(root, "messages.jsonl")
			require.NoError(t, err)
			defer func() { _ = file.Close() }()
			switch mode {
			case "replacement", "symlink":
				require.NoError(t, os.Rename(filepath.Join(dir, "messages.jsonl"), filepath.Join(dir, "retained")))
				if mode == "replacement" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "messages.jsonl"), []byte(projectionTestMessages), 0o600))
				} else {
					require.NoError(t, os.Symlink("retained", filepath.Join(dir, "messages.jsonl")))
				}
			case "size":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "messages.jsonl"), []byte(projectionTestMessages+"\n"), 0o600))
			case "closed":
				require.NoError(t, file.Close())
			}
			require.Error(t, checkProjectionFile(root, "messages.jsonl", file, info))
		})
	}
}

var errProjectionFault = errors.New("synthetic file failure")

type projectionFaultFile struct {
	*os.File
	mode  string
	calls []string
}

func (f *projectionFaultFile) Write(body []byte) (int, error) {
	f.calls = append(f.calls, "write")
	if f.mode == "write" || f.mode == "short" {
		n, err := f.File.Write(body[:1])
		if err != nil {
			return n, err
		}
		if f.mode == "write" {
			return n, errProjectionFault
		}
		return n, nil
	}
	return f.File.Write(body)
}

func (f *projectionFaultFile) Sync() error {
	f.calls = append(f.calls, "sync")
	if err := f.File.Sync(); err != nil {
		return err
	}
	if f.mode == "sync" {
		return errProjectionFault
	}
	return nil
}

func (f *projectionFaultFile) Close() error {
	f.calls = append(f.calls, "close")
	if err := f.File.Close(); err != nil {
		return err
	}
	if f.mode == "close" {
		return errProjectionFault
	}
	return nil
}

func TestProjectionChecksWriteSyncClose(t *testing.T) {
	for _, mode := range []string{"write", "short", "sync", "close", "success"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partial")
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			require.NoError(t, err)
			fault := &projectionFaultFile{File: file, mode: mode}
			err = finishProjectionFile(fault, func(w io.Writer) error { return writeProjectionBytes(w, []byte("synthetic content")) })
			if mode == "success" {
				require.NoError(t, err)
			} else if mode == "short" {
				require.ErrorIs(t, err, io.ErrShortWrite)
			} else {
				require.ErrorIs(t, err, errProjectionFault)
			}
			body, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			if mode == "write" || mode == "short" {
				require.Equal(t, "s", string(body))
				require.Equal(t, []string{"write", "close"}, fault.calls)
			} else {
				require.Equal(t, "synthetic content", string(body))
				require.Equal(t, []string{"write", "sync", "close"}, fault.calls)
			}
			_, err = file.Stat()
			require.Error(t, err)
		})
	}
}

func TestProjectionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "projection")
	receipt, err := WriteProjection(ctx, dir, projectionTestExpected(), projectionTestRevision)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, receipt)
	require.NoDirExists(t, dir)
	writeManualProjection(t, dir, projectionTestMessages, projectionTestManifest(projectionTestMessages))
	receipt, err = VerifyProjection(ctx, dir, projectionTestExpected(), projectionTestRevision)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, receipt)
}
