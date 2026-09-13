package share

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/slacrawl/internal/media"
)

func exportMediaLocked(ctx context.Context, db *sql.DB, opts Options) (*MediaManifest, error) {
	if strings.TrimSpace(opts.CacheDir) == "" {
		return nil, nil
	}
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "media")); err != nil {
		return nil, fmt.Errorf("reset media dir: %w", err)
	}
	rows, err := db.QueryContext(ctx, `
select f.workspace_id, f.channel_id, f.ts, f.file_id, coalesce(f.media_path, ''), coalesce(f.content_sha256, '')
from message_files f
join channels c on c.id = f.channel_id and c.workspace_id = f.workspace_id
where coalesce(f.media_path, '') <> ''
  and c.kind in ('public_channel', 'desktop_channel')
order by f.media_path, f.channel_id, f.ts, f.file_id
`)
	if err != nil {
		return nil, fmt.Errorf("query media files: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest := &MediaManifest{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var workspaceID, channelID, ts, fileID, mediaPath, expectedHash string
		if err := rows.Scan(&workspaceID, &channelID, &ts, &fileID, &mediaPath, &expectedHash); err != nil {
			return nil, err
		}
		if _, ok := seen[mediaPath]; ok {
			manifest.Files++
			continue
		}
		source, info, err := media.VerifiedFile(opts.CacheDir, mediaPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		// An unsafe path is a broken cache, not an absent file. Skipping it
		// silently let publish emit a manifest claiming the archive has no
		// media at all, which wipes attachments for every subscriber.
		if err != nil {
			return nil, fmt.Errorf("stat media %s: %w", mediaPath, err)
		}
		manifest.Files++
		seen[mediaPath] = struct{}{}
		rel := filepath.ToSlash(filepath.Join("media", mediaPath))
		target, err := media.RepoPath(opts.RepoPath, mediaPath)
		if err != nil {
			return nil, err
		}
		if err := copyFile(target, source); err != nil {
			return nil, fmt.Errorf("copy media %s: %w", mediaPath, err)
		}
		hash, err := fileSHA256(target)
		if err != nil {
			return nil, err
		}
		if expectedHash != "" && hash != expectedHash {
			return nil, fmt.Errorf("media hash mismatch for %s: got %s want %s", mediaPath, hash, expectedHash)
		}
		manifest.Items = append(manifest.Items, MediaFileManifest{Path: rel, Size: info.Size(), SHA256: hash})
		manifest.Bytes += info.Size()
		_ = workspaceID
		_ = channelID
		_ = ts
		_ = fileID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(manifest.Items) == 0 {
		return nil, nil
	}
	return manifest, nil
}

func importMedia(ctx context.Context, opts Options, manifest *MediaManifest) (int, error) {
	if manifest == nil || strings.TrimSpace(opts.CacheDir) == "" {
		return 0, nil
	}
	copied := 0
	for _, item := range manifest.Items {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		mediaPath, ok := strings.CutPrefix(filepath.ToSlash(item.Path), "media/")
		if !ok || strings.TrimSpace(mediaPath) == "" {
			return copied, fmt.Errorf("invalid media manifest path %q", item.Path)
		}
		source, _, err := media.VerifiedFile(opts.RepoPath, mediaPath)
		if err != nil {
			return copied, err
		}
		hash, err := fileSHA256(source)
		if err != nil {
			return copied, fmt.Errorf("hash media %s: %w", item.Path, err)
		}
		if item.SHA256 != "" && hash != item.SHA256 {
			return copied, fmt.Errorf("media hash mismatch for %s: got %s want %s", item.Path, hash, item.SHA256)
		}
		target, err := media.LocalPath(opts.CacheDir, mediaPath)
		if err != nil {
			return copied, err
		}
		if sameFileHash(target, hash) {
			continue
		}
		if err := copyFile(target, source); err != nil {
			return copied, fmt.Errorf("copy media %s: %w", item.Path, err)
		}
		copied++
	}
	return copied, nil
}

func mediaMetadataMissing(ctx context.Context, db *sql.DB, manifest *MediaManifest) (bool, error) {
	if manifest == nil || len(manifest.Items) == 0 {
		return false, nil
	}
	for _, item := range manifest.Items {
		mediaPath, ok := strings.CutPrefix(filepath.ToSlash(item.Path), "media/")
		if !ok || strings.TrimSpace(mediaPath) == "" {
			continue
		}
		var found int
		err := db.QueryRowContext(ctx, `select 1 from message_files where media_path = ? limit 1`, mediaPath).Scan(&found)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

type fileMediaRecord struct {
	MediaPath     string
	ContentSHA256 string
	ContentSize   int64
	FetchedAt     string
	FetchStatus   string
	FetchError    string
}

func fileMediaByKey(ctx context.Context, tx *sql.Tx) (map[string]fileMediaRecord, error) {
	rows, err := tx.QueryContext(ctx, `
select channel_id, ts, file_id, coalesce(media_path, ''), coalesce(content_sha256, ''),
       content_size, coalesce(fetched_at, ''), fetch_status, fetch_error
from message_files
where coalesce(media_path, '') <> ''
`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]fileMediaRecord{}
	for rows.Next() {
		var channelID, ts, fileID string
		var record fileMediaRecord
		if err := rows.Scan(&channelID, &ts, &fileID, &record.MediaPath, &record.ContentSHA256, &record.ContentSize, &record.FetchedAt, &record.FetchStatus, &record.FetchError); err != nil {
			return nil, err
		}
		out[fileMediaKey(channelID, ts, fileID)] = record
	}
	return out, rows.Err()
}

func clearUnmanifestedFileMedia(ctx context.Context, tx *sql.Tx, manifest *MediaManifest) error {
	manifested := map[string]struct{}{}
	if manifest != nil {
		for _, item := range manifest.Items {
			mediaPath, ok := strings.CutPrefix(filepath.ToSlash(item.Path), "media/")
			if ok && strings.TrimSpace(mediaPath) != "" {
				manifested[mediaPath] = struct{}{}
			}
		}
	}
	rows, err := tx.QueryContext(ctx, `
select channel_id, ts, file_id, coalesce(media_path, '')
from message_files
where coalesce(media_path, '') <> ''
`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	type key struct {
		channelID string
		ts        string
		fileID    string
	}
	clear := []key{}
	for rows.Next() {
		var channelID, ts, fileID, mediaPath string
		if err := rows.Scan(&channelID, &ts, &fileID, &mediaPath); err != nil {
			return err
		}
		if _, ok := manifested[mediaPath]; !ok {
			clear = append(clear, key{channelID: channelID, ts: ts, fileID: fileID})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(clear) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
update message_files
set media_path = null, content_sha256 = null, content_size = 0, fetched_at = null,
    fetch_status = '', fetch_error = ''
where channel_id = ? and ts = ? and file_id = ?
`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, row := range clear {
		if _, err := stmt.ExecContext(ctx, row.channelID, row.ts, row.fileID); err != nil {
			return err
		}
	}
	return nil
}

func clearUnmanifestedImportedFileMedia(ctx context.Context, tx *sql.Tx, manifest *MediaManifest, imported map[string]struct{}) error {
	if len(imported) == 0 {
		return nil
	}
	manifested := map[string]struct{}{}
	if manifest != nil {
		for _, item := range manifest.Items {
			mediaPath, ok := strings.CutPrefix(filepath.ToSlash(item.Path), "media/")
			if ok && strings.TrimSpace(mediaPath) != "" {
				manifested[mediaPath] = struct{}{}
			}
		}
	}
	stmt, err := tx.PrepareContext(ctx, `
update message_files
set media_path = null, content_sha256 = null, content_size = 0, fetched_at = null,
    fetch_status = '', fetch_error = ''
where channel_id = ? and ts = ? and file_id = ?
  and coalesce(media_path, '') <> ''
  and coalesce(media_path, '') = ?
`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for key := range imported {
		channelID, ts, fileID, ok := splitFileMediaKey(key)
		if !ok {
			continue
		}
		var mediaPath string
		err := tx.QueryRowContext(ctx, `
select coalesce(media_path, '') from message_files
where channel_id = ? and ts = ? and file_id = ?
`, channelID, ts, fileID).Scan(&mediaPath)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if mediaPath == "" {
			continue
		}
		if _, ok := manifested[mediaPath]; ok {
			continue
		}
		if _, err := stmt.ExecContext(ctx, channelID, ts, fileID, mediaPath); err != nil {
			return err
		}
	}
	return nil
}

func preserveImportedFileMedia(ctx context.Context, tx *sql.Tx, existing map[string]fileMediaRecord) error {
	if len(existing) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
update message_files
set media_path = ?, content_sha256 = ?, content_size = ?, fetched_at = ?,
    fetch_status = ?, fetch_error = ?
where channel_id = ? and ts = ? and file_id = ? and coalesce(media_path, '') = ''
`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for key, record := range existing {
		channelID, ts, fileID, ok := splitFileMediaKey(key)
		if !ok {
			continue
		}
		if _, err := stmt.ExecContext(ctx, record.MediaPath, nullableString(record.ContentSHA256), record.ContentSize, nullableString(record.FetchedAt), record.FetchStatus, record.FetchError, channelID, ts, fileID); err != nil {
			return err
		}
	}
	return nil
}

func fileMediaKey(channelID, ts, fileID string) string {
	return channelID + "\x00" + ts + "\x00" + fileID
}

func splitFileMediaKey(key string) (string, string, string, bool) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func copyFile(dst, src string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src) //nolint:gosec // Source path is validated by caller.
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".copy-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path) //nolint:gosec // Caller confines path.
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func sameFileHash(path, hash string) bool {
	current, err := fileSHA256(path)
	return err == nil && current == hash
}
