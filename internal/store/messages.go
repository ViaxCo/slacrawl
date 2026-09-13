package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

func (s *Store) UpsertMessage(ctx context.Context, message Message, mentions []Mention) error {
	_, err := s.upsertMessage(ctx, message, mentions, false, false)
	return err
}

// UpsertMessageByPriority atomically skips updates from a lower-priority source.
func (s *Store) UpsertMessageByPriority(ctx context.Context, message Message, mentions []Mention) (bool, error) {
	return s.upsertMessage(ctx, message, mentions, true, false)
}

func (s *Store) UpsertMessageWithRetention(ctx context.Context, message Message, mentions []Mention) (bool, error) {
	return s.upsertMessage(ctx, message, mentions, false, true)
}

func (s *Store) UpsertMessageByPriorityWithRetention(ctx context.Context, message Message, mentions []Mention) (bool, error) {
	return s.upsertMessage(ctx, message, mentions, true, true)
}

func (s *Store) upsertMessage(ctx context.Context, message Message, mentions []Mention, preserveHigherPriority, enforceRetention bool) (bool, error) {
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, enforceRetention)
	if err != nil {
		return false, err
	}
	defer rollback()
	written, err := upsertMessageInTransaction(ctx, dbtx, storedb.New(dbtx), message, mentions, preserveHigherPriority, enforceRetention)
	if err != nil {
		return false, err
	}
	if !written {
		return false, nil
	}
	if err := commit(); err != nil {
		return false, err
	}
	return written, nil
}

func upsertMessageInTransaction(ctx context.Context, dbtx storedb.DBTX, qtx *storedb.Queries, message Message, mentions []Mention, preserveHigherPriority, enforceRetention bool) (bool, error) {
	key := messageKey(message.ChannelID, message.TS)
	if enforceRetention {
		allowed, err := messageAllowedByRetention(ctx, dbtx, message)
		if err != nil {
			return false, err
		}
		if !allowed {
			return false, nil
		}
	}
	var err error
	var rows int64
	if preserveHigherPriority {
		rows, err = qtx.UpsertMessageByPriority(ctx, storedb.UpsertMessageByPriorityParams{
			ChannelID: message.ChannelID, Ts: message.TS, WorkspaceID: message.WorkspaceID,
			UserID: dbText(message.UserID), Subtype: dbText(message.Subtype), ClientMsgID: dbText(message.ClientMsgID),
			ThreadTs: dbText(message.ThreadTS), ParentUserID: dbText(message.ParentUserID), Text: message.Text,
			NormalizedText: message.NormalizedText, ReplyCount: int64(message.ReplyCount), LatestReply: dbText(message.LatestReply),
			EditedTs: dbText(message.EditedTS), DeletedTs: dbText(message.DeletedTS), SourceRank: int64(message.SourceRank),
			SourceName: message.SourceName, RawJson: message.RawJSON, UpdatedAt: formatDBTime(message.UpdatedAt),
		})
	} else {
		rows, err = qtx.UpsertMessage(ctx, storedb.UpsertMessageParams{
			ChannelID: message.ChannelID, Ts: message.TS, WorkspaceID: message.WorkspaceID,
			UserID: dbText(message.UserID), Subtype: dbText(message.Subtype), ClientMsgID: dbText(message.ClientMsgID),
			ThreadTs: dbText(message.ThreadTS), ParentUserID: dbText(message.ParentUserID), Text: message.Text,
			NormalizedText: message.NormalizedText, ReplyCount: int64(message.ReplyCount), LatestReply: dbText(message.LatestReply),
			EditedTs: dbText(message.EditedTS), DeletedTs: dbText(message.DeletedTS), SourceRank: int64(message.SourceRank),
			SourceName: message.SourceName, RawJson: message.RawJSON, UpdatedAt: formatDBTime(message.UpdatedAt),
		})
	}
	if err != nil {
		return false, err
	}
	if rows == 0 {
		if err := rejectMessageWorkspaceCollision(ctx, qtx, message); err != nil {
			return false, err
		}
		if preserveHigherPriority {
			return false, nil
		}
		return false, fmt.Errorf("message %q upsert affected no rows", key)
	}

	if err := replaceMessageMentions(ctx, qtx, message, mentions); err != nil {
		return false, err
	}

	var filesForSearch []MessageFile
	if message.Files != nil {
		existingMedia, err := existingFileMedia(ctx, qtx, message.ChannelID, message.TS)
		if err != nil {
			return false, err
		}
		if err := tombstoneMissingMessageFiles(ctx, dbtx, message, message.Files); err != nil {
			return false, err
		}
		for i, file := range message.Files {
			if file.WorkspaceID == "" {
				file.WorkspaceID = message.WorkspaceID
			}
			if file.ChannelID == "" {
				file.ChannelID = message.ChannelID
			}
			if file.TS == "" {
				file.TS = message.TS
			}
			if file.UserID == "" {
				file.UserID = message.UserID
			}
			if file.UpdatedAt.IsZero() {
				file.UpdatedAt = message.UpdatedAt
			}
			if media, ok := existingMedia[file.FileID]; ok && file.MediaPath == "" {
				file.MediaPath = media.MediaPath
				file.ContentSHA256 = media.ContentSHA256
				file.ContentSize = media.ContentSize
				file.FetchedAt = media.FetchedAt
				file.FetchStatus = media.FetchStatus
				file.FetchError = media.FetchError
			}
			message.Files[i] = file
			if err := qtx.InsertMessageFile(ctx, insertMessageFileParams(file)); err != nil {
				return false, err
			}
		}
		filesForSearch = message.Files
	} else {
		filesForSearch, err = existingFilesForSearch(ctx, dbtx, message.ChannelID, message.TS)
		if err != nil {
			return false, err
		}
	}

	searchMessage := message
	searchMessage.Files = filesForSearch
	if err := writeMessageFTS(ctx, dbtx, message.ChannelID, message.TS, messageSearchContent(searchMessage)); err != nil {
		return false, err
	}

	if err := appendMessageEvent(ctx, qtx, message, formatDBTime(message.UpdatedAt)); err != nil {
		return false, err
	}

	return true, nil
}

type messageFTSWriter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func writeMessageFTS(ctx context.Context, writer messageFTSWriter, channelID, ts, content string) error {
	result, err := writer.ExecContext(ctx, upsertMessageFTSRowSQL, content, channelID, ts)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("message %q search index upsert affected %d rows", messageKey(channelID, ts), rows)
	}
	return nil
}

func (s *Store) MarkMessageDeleted(ctx context.Context, message Message, mentions []Mention) error {
	_, err := s.markMessageDeleted(ctx, message, mentions, false)
	return err
}

func (s *Store) MarkMessageDeletedWithRetention(ctx context.Context, message Message, mentions []Mention) (bool, error) {
	return s.markMessageDeleted(ctx, message, mentions, true)
}

func (s *Store) DeleteMessageBySource(ctx context.Context, workspaceID, channelID, ts, sourceName string) (bool, error) {
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return false, err
	}
	defer rollback()

	var exists bool
	if err := dbtx.QueryRowContext(ctx, `
select exists (
  select 1
  from messages
  where workspace_id = ?
    and channel_id = ?
    and ts = ?
    and source_name = ?
)
`, workspaceID, channelID, ts, sourceName).Scan(&exists); err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	for _, query := range []string{
		`delete from message_events where channel_id = ? and ts = ?`,
		`delete from message_event_heads where channel_id = ? and ts = ?`,
		`delete from message_files where channel_id = ? and ts = ?`,
		`delete from message_mentions where channel_id = ? and ts = ?`,
		`delete from embedding_jobs where channel_id = ? and ts = ?`,
	} {
		if _, err := dbtx.ExecContext(ctx, query, channelID, ts); err != nil {
			return false, err
		}
	}
	if _, err := dbtx.ExecContext(ctx, deleteMessageFTSRowSQL, channelID, ts); err != nil {
		return false, err
	}
	if _, err := dbtx.ExecContext(ctx, `
delete from messages
where workspace_id = ? and channel_id = ? and ts = ? and source_name = ?
`, workspaceID, channelID, ts, sourceName); err != nil {
		return false, err
	}
	if err := commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) markMessageDeleted(ctx context.Context, message Message, mentions []Mention, enforceRetention bool) (bool, error) {
	key := messageKey(message.ChannelID, message.TS)
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, enforceRetention)
	if err != nil {
		return false, err
	}
	defer rollback()
	if enforceRetention {
		allowed, err := messageAllowedByRetention(ctx, dbtx, message)
		if err != nil {
			return false, err
		}
		if !allowed {
			return false, nil
		}
	}
	qtx := storedb.New(dbtx)
	deleteWins, err := messageDeleteWins(ctx, dbtx, message)
	if err != nil {
		return false, err
	}
	if !deleteWins {
		return false, nil
	}

	updatedAt := formatDBTime(message.UpdatedAt)
	rows, err := qtx.MarkMessageDeleted(ctx, storedb.MarkMessageDeletedParams{
		DeletedTs:   dbText(message.DeletedTS),
		UpdatedAt:   updatedAt,
		ChannelID:   message.ChannelID,
		Ts:          message.TS,
		WorkspaceID: message.WorkspaceID,
	})
	if err != nil {
		return false, err
	}
	switch rows {
	case 0:
		rows, err := qtx.UpsertMessage(ctx, storedb.UpsertMessageParams{
			ChannelID:      message.ChannelID,
			Ts:             message.TS,
			WorkspaceID:    message.WorkspaceID,
			UserID:         dbText(message.UserID),
			Subtype:        dbText(message.Subtype),
			ClientMsgID:    dbText(message.ClientMsgID),
			ThreadTs:       dbText(message.ThreadTS),
			ParentUserID:   dbText(message.ParentUserID),
			Text:           message.Text,
			NormalizedText: message.NormalizedText,
			ReplyCount:     int64(message.ReplyCount),
			LatestReply:    dbText(message.LatestReply),
			EditedTs:       dbText(message.EditedTS),
			DeletedTs:      dbText(message.DeletedTS),
			SourceRank:     int64(message.SourceRank),
			SourceName:     message.SourceName,
			RawJson:        message.RawJSON,
			UpdatedAt:      updatedAt,
		})
		if err != nil {
			return false, err
		}
		if rows == 0 {
			if err := rejectMessageWorkspaceCollision(ctx, qtx, message); err != nil {
				return false, err
			}
			return false, fmt.Errorf("message %q upsert affected no rows", key)
		}
		searchMessage := message
		searchMessage.Files = nil
		if err := writeMessageFTS(ctx, dbtx, message.ChannelID, message.TS, messageSearchContent(searchMessage)); err != nil {
			return false, err
		}
		if err := replaceMessageMentions(ctx, qtx, message, mentions); err != nil {
			return false, err
		}
	default:
		normalizedText, err := qtx.GetMessageSearchText(ctx, storedb.GetMessageSearchTextParams{ChannelID: message.ChannelID, Ts: message.TS})
		if err != nil {
			return false, err
		}
		searchMessage := message
		searchMessage.NormalizedText = normalizedText
		searchMessage.Files = nil
		if err := writeMessageFTS(ctx, dbtx, message.ChannelID, message.TS, messageSearchContent(searchMessage)); err != nil {
			return false, err
		}
	}
	if _, err := dbtx.ExecContext(ctx, `
update message_files
set deleted_at = ?, deletion_source = ?, deletion_reason = 'parent_message_deleted', updated_at = ?
where channel_id = ? and ts = ?
`, updatedAt, message.SourceName, updatedAt, message.ChannelID, message.TS); err != nil {
		return false, err
	}
	if _, err := dbtx.ExecContext(ctx, `
update message_mentions
set deleted_at = ?, deletion_source = ?, deletion_reason = 'parent_message_deleted', updated_at = ?
where channel_id = ? and ts = ?
`, updatedAt, message.SourceName, updatedAt, message.ChannelID, message.TS); err != nil {
		return false, err
	}
	if err := appendMessageEvent(ctx, qtx, message, updatedAt); err != nil {
		return false, err
	}
	if err := commit(); err != nil {
		return false, err
	}
	return true, nil
}

func messageDeleteWins(ctx context.Context, dbtx storedb.DBTX, message Message) (bool, error) {
	var workspaceID, updatedAt string
	var sourceRank int64
	var deletedTS sql.NullString
	err := dbtx.QueryRowContext(ctx, `
select workspace_id, updated_at, source_rank, deleted_ts
from messages
where channel_id = ? and ts = ?
`, message.ChannelID, message.TS).Scan(&workspaceID, &updatedAt, &sourceRank, &deletedTS)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if workspaceID != message.WorkspaceID {
		return false, &WorkspaceCollisionError{Entity: "message", ID: messageKey(message.ChannelID, message.TS), ExistingWorkspaceID: workspaceID, WorkspaceID: message.WorkspaceID}
	}
	incomingUpdatedAt := formatDBTime(message.UpdatedAt)
	comparison := compareDBTimes(incomingUpdatedAt, updatedAt)
	if comparison != 0 {
		return comparison > 0, nil
	}
	if strings.TrimSpace(deletedTS.String) != "" {
		return true, nil
	}
	return int64(message.SourceRank) <= sourceRank, nil
}

func compareDBTimes(left, right string) int {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	if leftErr == nil && rightErr == nil {
		switch {
		case leftTime.Before(rightTime):
			return -1
		case leftTime.After(rightTime):
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
}

func (s *Store) beginMessageTransaction(ctx context.Context, immediate bool) (storedb.DBTX, func() error, func(), error) {
	if !immediate {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, nil, nil, err
		}
		return tx, tx.Commit, func() { _ = tx.Rollback() }, nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, "begin immediate"); err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	done := false
	commit := func() error {
		if _, err := conn.ExecContext(ctx, "commit"); err != nil {
			return err
		}
		done = true
		return conn.Close()
	}
	rollback := func() {
		if !done {
			_, _ = conn.ExecContext(context.Background(), "rollback")
		}
		_ = conn.Close()
	}
	return conn, commit, rollback, nil
}

func rejectMessageWorkspaceCollision(ctx context.Context, q *storedb.Queries, message Message) error {
	existing, err := q.GetMessageWorkspace(ctx, storedb.GetMessageWorkspaceParams{ChannelID: message.ChannelID, Ts: message.TS})
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing != message.WorkspaceID {
		return &WorkspaceCollisionError{Entity: "message", ID: messageKey(message.ChannelID, message.TS), ExistingWorkspaceID: existing, WorkspaceID: message.WorkspaceID}
	}
	return nil
}

func rejectWorkspaceCollision(ctx context.Context, workspaceID, entity, id string, lookup func(context.Context, string) (string, error)) error {
	existing, err := lookup(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing != workspaceID {
		return &WorkspaceCollisionError{Entity: entity, ID: id, ExistingWorkspaceID: existing, WorkspaceID: workspaceID}
	}
	return nil
}

func replaceMessageMentions(ctx context.Context, qtx *storedb.Queries, message Message, mentions []Mention) error {
	if err := qtx.TombstoneMessageMentions(ctx, storedb.TombstoneMessageMentionsParams{
		DeletedAt:      dbText(formatDBTime(message.UpdatedAt)),
		DeletionSource: dbText(message.SourceName),
		UpdatedAt:      formatDBTime(message.UpdatedAt),
		ChannelID:      message.ChannelID,
		Ts:             message.TS,
	}); err != nil {
		return err
	}
	seenMentions := map[string]struct{}{}
	for _, mention := range mentions {
		key := mention.Type + "|" + mention.TargetID + "|" + mention.DisplayText
		if _, ok := seenMentions[key]; ok {
			continue
		}
		seenMentions[key] = struct{}{}
		if err := qtx.UpsertMessageMention(ctx, storedb.UpsertMessageMentionParams{
			ChannelID:   message.ChannelID,
			Ts:          message.TS,
			MentionType: mention.Type,
			TargetID:    mention.TargetID,
			DisplayText: dbText(mention.DisplayText),
			UpdatedAt:   formatDBTime(message.UpdatedAt),
		}); err != nil {
			return err
		}
	}
	return nil
}

func tombstoneMissingMessageFiles(ctx context.Context, dbtx storedb.DBTX, message Message, files []MessageFile) error {
	active := make(map[string]struct{}, len(files))
	for _, file := range files {
		active[file.FileID] = struct{}{}
	}
	rows, err := dbtx.QueryContext(ctx, `select file_id from message_files where channel_id = ? and ts = ?`, message.ChannelID, message.TS)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var missing []string
	for rows.Next() {
		var fileID string
		if err := rows.Scan(&fileID); err != nil {
			return err
		}
		if _, ok := active[fileID]; !ok {
			missing = append(missing, fileID)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, fileID := range missing {
		if _, err := dbtx.ExecContext(ctx, `
update message_files
set deleted_at = ?, deletion_source = ?, deletion_reason = 'absent_from_authoritative_message_payload', updated_at = ?
where channel_id = ? and ts = ? and file_id = ?
`, formatDBTime(message.UpdatedAt), message.SourceName, formatDBTime(message.UpdatedAt), message.ChannelID, message.TS, fileID); err != nil {
			return err
		}
	}
	return nil
}

func existingFileMedia(ctx context.Context, qtx *storedb.Queries, channelID, ts string) (map[string]MessageFile, error) {
	rows, err := qtx.ListExistingFileMedia(ctx, storedb.ListExistingFileMediaParams{ChannelID: channelID, Ts: ts})
	if err != nil {
		return nil, err
	}
	out := map[string]MessageFile{}
	for _, row := range rows {
		out[row.FileID] = MessageFile{
			FileID:        row.FileID,
			MediaPath:     row.MediaPath,
			ContentSHA256: row.ContentSha256,
			ContentSize:   row.ContentSize,
			FetchedAt:     row.FetchedAt,
			FetchStatus:   row.FetchStatus,
			FetchError:    row.FetchError,
		}
	}
	return out, nil
}

func existingFilesForSearch(ctx context.Context, q storedb.DBTX, channelID, ts string) ([]MessageFile, error) {
	rows, err := q.QueryContext(ctx, `
select file_id, name, title, plain_text, preview_plain_text
from message_files
where channel_id = ? and ts = ? and deleted_at is null
`, channelID, ts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	files := []MessageFile{}
	for rows.Next() {
		var file MessageFile
		if err := rows.Scan(&file.FileID, &file.Name, &file.Title, &file.PlainText, &file.PreviewPlainText); err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, rows.Err()
}

func insertMessageFileParams(file MessageFile) storedb.InsertMessageFileParams {
	return storedb.InsertMessageFileParams{
		WorkspaceID:        file.WorkspaceID,
		ChannelID:          file.ChannelID,
		Ts:                 file.TS,
		FileID:             file.FileID,
		UserID:             dbText(file.UserID),
		Name:               file.Name,
		Title:              file.Title,
		Mimetype:           dbText(file.Mimetype),
		Filetype:           dbText(file.Filetype),
		PrettyType:         dbText(file.PrettyType),
		Mode:               dbText(file.Mode),
		Size:               file.Size,
		UrlPrivate:         dbText(file.URLPrivate),
		UrlPrivateDownload: dbText(file.URLPrivateDownload),
		Permalink:          dbText(file.Permalink),
		IsPublic:           boolInt(file.IsPublic),
		PlainText:          file.PlainText,
		PreviewPlainText:   file.PreviewPlainText,
		MediaPath:          dbText(file.MediaPath),
		ContentSha256:      dbText(file.ContentSHA256),
		ContentSize:        file.ContentSize,
		FetchedAt:          dbText(file.FetchedAt),
		FetchStatus:        file.FetchStatus,
		FetchError:         file.FetchError,
		RawJson:            file.RawJSON,
		UpdatedAt:          formatDBTime(file.UpdatedAt),
	}
}

func eventType(message Message) string {
	switch {
	case message.DeletedTS != "":
		return "message_deleted"
	case message.EditedTS != "":
		return "message_changed"
	default:
		return "message"
	}
}

func appendMessageEvent(ctx context.Context, qtx *storedb.Queries, message Message, createdAt string) error {
	typeName := eventType(message)
	head, err := qtx.GetMessageEventHead(ctx, storedb.GetMessageEventHeadParams{
		ChannelID:  message.ChannelID,
		Ts:         message.TS,
		EventType:  typeName,
		SourceName: message.SourceName,
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read message event head: %w", err)
	}
	if err == nil && head == message.RawJSON {
		return nil
	}
	if err := qtx.InsertMessageEvent(ctx, storedb.InsertMessageEventParams{
		ChannelID:   message.ChannelID,
		Ts:          message.TS,
		EventType:   typeName,
		SourceName:  message.SourceName,
		PayloadJson: message.RawJSON,
		CreatedAt:   createdAt,
	}); err != nil {
		return err
	}
	if err := qtx.UpsertMessageEventHead(ctx, storedb.UpsertMessageEventHeadParams{
		ChannelID:   message.ChannelID,
		Ts:          message.TS,
		EventType:   typeName,
		SourceName:  message.SourceName,
		PayloadJson: message.RawJSON,
	}); err != nil {
		return fmt.Errorf("update message event head: %w", err)
	}
	return nil
}

func messageKey(channelID string, ts string) string {
	return strings.TrimSpace(channelID) + "|" + strings.TrimSpace(ts)
}

func messageSearchContent(message Message) string {
	parts := []string{message.NormalizedText}
	for _, file := range message.Files {
		parts = append(parts, file.Name, file.Title, file.PlainText, file.PreviewPlainText)
	}
	return strings.Join(filterNonEmpty(parts), " ")
}

func filterNonEmpty(parts []string) []string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			filtered = append(filtered, strings.TrimSpace(part))
		}
	}
	return filtered
}
