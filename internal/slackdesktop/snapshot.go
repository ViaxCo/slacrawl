package slackdesktop

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

func Discover(path string) (Source, error) {
	if path == "" {
		return Source{}, nil
	}
	source := Source{Path: path}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return source, nil
		}
		return Source{}, err
	}
	if !info.IsDir() {
		return Source{}, errors.New("desktop path is not a directory")
	}
	source.Available = true

	root, err := LoadRootState(filepath.Join(path, rootStateFile))
	if err != nil && !os.IsNotExist(err) {
		return Source{}, err
	}
	source.Summary = root.Summary
	return source, nil
}

func Inspect(ctx context.Context, path string) (Source, error) {
	source, err := Discover(path)
	if err != nil {
		return Source{}, err
	}
	if !source.Available {
		return source, nil
	}

	snapshot, err := SnapshotPath(path)
	if err != nil {
		return Source{}, err
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(snapshot.Root)) }()

	extracted, err := Extract(ctx, snapshot.Root)
	if err != nil {
		return Source{}, err
	}
	source.Summary = extracted.RootState.Summary
	source.Local = localSummary(extracted)
	source.IndexedDB = extracted.IndexedDB
	return source, nil
}

func SnapshotPath(path string) (snapshot Snapshot, err error) {
	root, err := makeSnapshotTempDir("", "slacrawl-desktop-*")
	if err != nil {
		return Snapshot{}, err
	}
	keepSnapshot := false
	defer func() {
		if !keepSnapshot {
			_ = os.RemoveAll(root)
		}
	}()

	target := filepath.Join(root, "Slack")
	if err := os.MkdirAll(target, 0o750); err != nil {
		return Snapshot{}, err
	}
	sourceRoot, err := os.OpenRoot(path)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { _ = sourceRoot.Close() }()
	targetRoot, err := os.OpenRoot(target)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { _ = targetRoot.Close() }()

	copyTargets := []string{
		rootStateFile,
		"local-settings.json",
		localStorageDir,
		indexedDBDir,
		indexedDBBlobDir,
	}
	for _, relative := range copyTargets {
		relative = filepath.FromSlash(relative)
		if _, err := sourceRoot.Lstat(relative); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return Snapshot{}, err
		}
		if err := copyPath(sourceRoot, targetRoot, relative); err != nil {
			return Snapshot{}, err
		}
	}
	keepSnapshot = true
	return Snapshot{Root: target}, nil
}

func Extract(ctx context.Context, path string) (ExtractedData, error) {
	root, err := LoadRootState(filepath.Join(path, rootStateFile))
	if err != nil && !os.IsNotExist(err) {
		return ExtractedData{}, err
	}

	local, err := ParseLocalStorage(filepath.Join(path, localStorageDir))
	if err != nil && !os.IsNotExist(err) {
		return ExtractedData{}, err
	}

	indexed, err := ScanIndexedDB(filepath.Join(path, indexedDBDir))
	if err != nil && !os.IsNotExist(err) {
		return ExtractedData{}, err
	}
	reduxStates, decodeSummary, err := extractIndexedDBStates(ctx, path)
	if err != nil {
		return ExtractedData{}, err
	}
	indexed.NodeAvailable = decodeSummary.NodeAvailable
	indexed.BlobFileCount = decodeSummary.BlobFileCount
	indexed.CandidateCount = decodeSummary.CandidateCount
	indexed.DecodedBlobCount = decodeSummary.DecodedBlobCount
	indexed.DecodedStateCount = decodeSummary.DecodedStateCount
	indexed.DecodeFailureCount = decodeSummary.DecodeFailureCount
	indexed.DecodeFailures = decodeSummary.DecodeFailures
	indexed.V8Versions = decodeSummary.V8Versions

	return ExtractedData{
		RootState:   root,
		LocalConfig: local.LocalConfig,
		Drafts:      local.Drafts,
		Activity:    local.Activity,
		Recent:      local.Recent,
		ReadMarkers: local.ReadMarkers,
		Statuses:    local.Statuses,
		Expandables: local.Expandables,
		ReduxStates: reduxStates,
		IndexedDB:   indexed,
	}, nil
}

func copyPath(srcRoot *os.Root, dstRoot *os.Root, relative string) error {
	info, err := srcRoot.Lstat(relative)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("desktop snapshot source contains a symbolic link")
	}
	if info.IsDir() {
		if err := dstRoot.MkdirAll(relative, info.Mode().Perm()); err != nil {
			return err
		}
		dir, err := srcRoot.Open(relative)
		if err != nil {
			return err
		}
		entries, readErr := dir.ReadDir(-1)
		closeErr := dir.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		for _, entry := range entries {
			if err := copyPath(srcRoot, dstRoot, filepath.Join(relative, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		return errors.New("desktop snapshot source contains a special file")
	}
	if err := dstRoot.MkdirAll(filepath.Dir(relative), 0o750); err != nil {
		return err
	}
	data, err := srcRoot.ReadFile(relative)
	if err != nil {
		return err
	}
	return dstRoot.WriteFile(relative, data, info.Mode().Perm())
}
