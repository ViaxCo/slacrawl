package share

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/media"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func admissionSnapshot(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	snapshot := map[string][]map[string]any{}
	for _, table := range append(append([]string{}, SnapshotTables...), "message_event_heads", "message_fts") {
		orderBy := "rowid"
		if table == "message_event_heads" {
			// This WITHOUT ROWID table needs its full primary key for stable snapshots.
			orderBy = "channel_id,ts,event_type,source_name"
		}
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table+" order by "+orderBy)
		require.NoError(t, err, "snapshot table %s", table)
		snapshot[table] = rows
	}
	return snapshot
}

func TestExportPolicyRejectsBeforeOwnerSideEffects(t *testing.T) {
	for _, includeMedia := range []bool{false, true} {
		name := "no-media"
		if includeMedia {
			name = "media"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{
				RepoPath: filepath.Join(dir, "repo"), Remote: filepath.Join(dir, "remote.git"),
				CacheDir: filepath.Join(dir, "cache"), IncludeMedia: includeMedia, DMPolicy: admission.Exclude,
			}
			t.Setenv("PATH", t.TempDir())
			t.Setenv("TMPDIR", filepath.Join(dir, "absent-temp"))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for range 2 {
				// A nil Store and unavailable Git must never precede the policy error.
				manifest, err := Export(ctx, nil, opts)
				require.EqualError(t, err, "legacy Git share exports cannot enforce sync.include_dms=false; keep this archive local")
				require.Equal(t, Manifest{}, manifest)
				for _, path := range []string{opts.RepoPath, opts.Remote, opts.CacheDir, os.Getenv("TMPDIR")} {
					require.NoDirExists(t, path)
					require.NoFileExists(t, path)
				}
			}
		})
	}
}

func TestImportPolicyRejectsBeforeOwnerSideEffects(t *testing.T) {
	for _, manifest := range []string{"missing", "malformed"} {
		for _, entry := range []string{"import", "restore", "if-changed", "historical", "blank-ref"} {
			t.Run(manifest+"/"+entry, func(t *testing.T) {
				dir := t.TempDir()
				st := seedStore(t, filepath.Join(dir, "reader.db"))
				defer func() { require.NoError(t, st.Close()) }()
				require.NoError(t, st.SetSyncState(context.Background(), "share", "import", "last_import_at", "2000-01-01T00:00:00Z"))
				before := admissionSnapshot(t, st)
				opts := Options{RepoPath: filepath.Join(dir, "repo"), CacheDir: filepath.Join(dir, "cache"), IncludeMedia: true, DMPolicy: admission.Exclude}
				if manifest == "malformed" {
					require.NoError(t, os.MkdirAll(opts.RepoPath, 0o700))
					require.NoError(t, os.WriteFile(filepath.Join(opts.RepoPath, ManifestName), []byte("not a manifest"), 0o600))
				}
				// A policy rejection must precede both cache locking and Git/ref reads.
				t.Setenv("PATH", t.TempDir())
				t.Setenv("TMPDIR", filepath.Join(dir, "absent-temp"))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				for range 2 {
					var err error
					switch entry {
					case "import":
						_, err = Import(ctx, st, opts)
					case "restore":
						_, err = Restore(ctx, st, opts)
					case "if-changed":
						var changed bool
						_, changed, err = ImportIfChanged(ctx, st, opts)
						require.False(t, changed)
					case "historical":
						_, err = RestoreAt(ctx, st, opts, "snapshot")
					case "blank-ref":
						_, err = RestoreAt(ctx, st, opts, "  ")
					}
					require.ErrorContains(t, err, "legacy Git share imports cannot enforce sync.include_dms=false")
					require.ErrorContains(t, err, "share.auto_update=false")
					require.Equal(t, before, admissionSnapshot(t, st))
					require.NoDirExists(t, opts.CacheDir)
					require.NoDirExists(t, filepath.Join(dir, "absent-temp"))
					if manifest == "missing" {
						require.NoDirExists(t, opts.RepoPath)
					} else {
						body, readErr := os.ReadFile(filepath.Join(opts.RepoPath, ManifestName))
						require.NoError(t, readErr)
						require.Equal(t, "not a manifest", string(body))
					}
				}
			})
		}
	}
}

func TestImportPolicyGuardsUnchangedManifestMediaAndFreshness(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(dir, "absent-gitconfig"))
	t.Setenv("GIT_CONFIG_COUNT", "0")
	t.Setenv("GIT_ALLOW_PROTOCOL", "file")
	source := seedStore(t, filepath.Join(dir, "source.db"))
	defer func() { require.NoError(t, source.Close()) }()
	now := time.Now().UTC()
	require.NoError(t, source.UpsertChannel(ctx, store.Channel{ID: "DDM", WorkspaceID: "T1", Name: "direct", Kind: "im", RawJSON: "{}", UpdatedAt: now}))
	require.NoError(t, source.UpsertMessage(ctx, store.Message{
		ChannelID: "DDM", TS: "123.900", WorkspaceID: "T1", UserID: "U1",
		Text: "share-dm-canary", NormalizedText: "share-dm-canary", RawJSON: "{\"text\":\"share-dm-canary\"}",
		SourceRank: 2, SourceName: "api-bot", UpdatedAt: now,
	}, nil))
	body := []byte("synthetic cached public file")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	mediaPath := "files/" + hash[:2] + "/" + hash + "-fixture.txt"
	sourceCache := filepath.Join(dir, "source-cache")
	sourceFile, err := media.LocalPath(sourceCache, mediaPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(sourceFile), 0o700))
	require.NoError(t, os.WriteFile(sourceFile, body, 0o600))
	require.NoError(t, source.UpsertMessage(ctx, store.Message{
		ChannelID: "C1", TS: "123.789", WorkspaceID: "T1", UserID: "U1",
		Text: "public media", NormalizedText: "public media", RawJSON: "{}",
		SourceRank: 2, SourceName: "api-bot", UpdatedAt: now,
		Files: []store.MessageFile{{FileID: "F1", Name: "fixture.txt", MediaPath: mediaPath, ContentSHA256: hash, ContentSize: int64(len(body)), FetchStatus: "fetched", RawJSON: "{}"}},
	}, nil))
	opts := Options{RepoPath: filepath.Join(dir, "repo"), CacheDir: sourceCache, IncludeMedia: true, DMPolicy: admission.Include}
	// An allowed publisher retains the DM canary for the importing policy checks.
	manifest, err := Export(ctx, source, opts)
	require.NoError(t, err)
	require.NotNil(t, manifest.Media)
	require.Len(t, manifest.Media.Items, 1)
	require.NoError(t, MarkImported(ctx, source, manifest))

	for _, mode := range []string{"missing-metadata", "missing-file", "no-media"} {
		for _, policy := range []struct {
			name  string
			value admission.DMPolicy
		}{{"omitted", admission.Default}, {"true", admission.Include}, {"false", admission.Exclude}} {
			t.Run(mode+"/"+policy.name, func(t *testing.T) {
				readerDir := t.TempDir()
				reader, err := store.Open(filepath.Join(readerDir, "reader.db"))
				require.NoError(t, err)
				defer func() { require.NoError(t, reader.Close()) }()
				readerOpts := Options{RepoPath: opts.RepoPath, CacheDir: filepath.Join(readerDir, "cache"), IncludeMedia: mode == "missing-file"}
				_, err = Import(ctx, reader, readerOpts)
				require.NoError(t, err)
				rows, err := reader.QueryReadOnly(ctx, "select text from messages where channel_id = 'DDM'")
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "share-dm-canary", rows[0]["text"])
				dstPath, err := media.LocalPath(readerOpts.CacheDir, mediaPath)
				require.NoError(t, err)
				if mode == "missing-file" {
					require.NoError(t, os.Remove(dstPath))
				}
				const oldImport = "2000-01-01T00:00:00Z"
				require.NoError(t, reader.SetSyncState(ctx, "share", "import", "last_import_at", oldImport))
				require.True(t, ManifestAlreadyImported(ctx, reader, manifest))
				before := admissionSnapshot(t, reader)
				readerOpts.IncludeMedia = mode != "no-media"
				readerOpts.DMPolicy = policy.value
				for range 2 {
					got, changed, err := ImportIfChanged(ctx, reader, readerOpts)
					require.False(t, changed)
					if policy.value == admission.Exclude {
						require.ErrorContains(t, err, "sync.include_dms=false")
						require.Equal(t, Manifest{}, got)
						require.Equal(t, before, admissionSnapshot(t, reader))
						require.NoFileExists(t, dstPath)
						continue
					}
					require.NoError(t, err)
					require.Equal(t, manifest.GeneratedAt, got.GeneratedAt)
					last, err := reader.GetSyncState(ctx, "share", "import", "last_import_at")
					require.NoError(t, err)
					require.NotEqual(t, oldImport, last)
					files, err := reader.Files(ctx, store.FileListOptions{FileID: "F1"})
					require.NoError(t, err)
					require.Len(t, files, 1)
					if mode == "no-media" {
						require.NoFileExists(t, dstPath)
						require.Empty(t, files[0].MediaPath)
					} else {
						restored, err := os.ReadFile(dstPath)
						require.NoError(t, err)
						require.Equal(t, body, restored)
						require.Equal(t, mediaPath, files[0].MediaPath)
					}
				}
			})
		}
	}
}

func TestImportPolicyDoesNotGateLocalGitPreparation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(dir, "absent-gitconfig"))
	t.Setenv("GIT_CONFIG_COUNT", "0")
	t.Setenv("GIT_ALLOW_PROTOCOL", "file")
	opts := Options{RepoPath: filepath.Join(dir, "repo"), Branch: "main", DMPolicy: admission.Exclude}
	require.NoError(t, EnsureRepo(context.Background(), opts))
	require.NoError(t, Pull(context.Background(), opts))
	require.DirExists(t, filepath.Join(opts.RepoPath, ".git"))
}
