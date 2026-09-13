package store

import (
	"context"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

func (s *Store) Files(ctx context.Context, opts FileListOptions) ([]FileRow, error) {
	args := []any{}
	clauses := []string{"1=1"}
	if opts.WorkspaceID != "" {
		clauses = append(clauses, "workspace_id = ?")
		args = append(args, opts.WorkspaceID)
	}
	if opts.ChannelID != "" {
		clauses = append(clauses, "channel_id = ?")
		args = append(args, opts.ChannelID)
	}
	if opts.UserID != "" {
		clauses = append(clauses, "coalesce(user_id, '') = ?")
		args = append(args, opts.UserID)
	}
	if opts.FileID != "" {
		clauses = append(clauses, "file_id = ?")
		args = append(args, opts.FileID)
	}
	if opts.Filename != "" {
		clauses = append(clauses, "(name like ? or title like ?)")
		like := "%" + opts.Filename + "%"
		args = append(args, like, like)
	}
	if opts.ContentType != "" {
		clauses = append(clauses, "(coalesce(mimetype, '') like ? or coalesce(filetype, '') like ?)")
		like := "%" + opts.ContentType + "%"
		args = append(args, like, like)
	}
	if !opts.Since.IsZero() {
		clauses = append(clauses, "ts >= ?")
		args = append(args, slackTSFromTime(opts.Since))
	}
	if !opts.Before.IsZero() {
		clauses = append(clauses, "ts < ?")
		args = append(args, slackTSFromTime(opts.Before))
	}
	query := `
select workspace_id, channel_id, ts, file_id, coalesce(user_id, ''), name, title,
       coalesce(mimetype, ''), coalesce(filetype, ''), coalesce(pretty_type, ''),
       coalesce(mode, ''), size, coalesce(url_private, ''), coalesce(url_private_download, ''),
       coalesce(permalink, ''), is_public, plain_text, preview_plain_text,
       coalesce(media_path, ''), coalesce(content_sha256, ''), content_size,
       coalesce(fetched_at, ''), fetch_status, fetch_error, updated_at
from message_files
where deleted_at is null and ` + strings.Join(clauses, " and ") + `
order by ts desc, file_id asc
`
	if opts.Limit > 0 {
		query += ` limit ?`
		args = append(args, opts.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []FileRow{}
	for rows.Next() {
		var row FileRow
		var isPublic int64
		var fetchedAt, updatedAt string
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.ChannelID,
			&row.TS,
			&row.FileID,
			&row.UserID,
			&row.Name,
			&row.Title,
			&row.Mimetype,
			&row.Filetype,
			&row.PrettyType,
			&row.Mode,
			&row.Size,
			&row.URLPrivate,
			&row.URLPrivateDownload,
			&row.Permalink,
			&isPublic,
			&row.PlainText,
			&row.PreviewPlainText,
			&row.MediaPath,
			&row.ContentSHA256,
			&row.ContentSize,
			&fetchedAt,
			&row.FetchStatus,
			&row.FetchError,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		row.IsPublic = isPublic != 0
		row.FetchedAt = parseDBTime(fetchedAt)
		row.UpdatedAt = parseDBTime(updatedAt)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) UpdateFileMedia(ctx context.Context, update FileMediaUpdate) error {
	return s.q.UpdateFileMedia(ctx, storedb.UpdateFileMediaParams{
		MediaPath:     dbText(update.MediaPath),
		ContentSha256: dbText(update.ContentSHA256),
		ContentSize:   update.ContentSize,
		FetchedAt:     dbText(update.FetchedAt),
		FetchStatus:   update.FetchStatus,
		FetchError:    update.FetchError,
		UpdatedAt:     formatDBTime(time.Now().UTC()),
		ChannelID:     update.ChannelID,
		Ts:            update.TS,
		FileID:        update.FileID,
	})
}

func (s *Store) UpdateFileFetchStatus(ctx context.Context, channelID, ts, fileID, fetchedAt, status, message string) error {
	return s.q.UpdateFileFetchStatus(ctx, storedb.UpdateFileFetchStatusParams{
		FetchedAt:   dbText(fetchedAt),
		FetchStatus: status,
		FetchError:  message,
		UpdatedAt:   formatDBTime(time.Now().UTC()),
		ChannelID:   channelID,
		Ts:          ts,
		FileID:      fileID,
	})
}
