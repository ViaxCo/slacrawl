package share

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/slacrawl/internal/store"
)

const ExportSelectionVersion = 1

type ExportTextMode string

const (
	ExportTextKeep    ExportTextMode = "keep"
	ExportTextReplace ExportTextMode = "replace"
)

type ExportTextChoice struct {
	Mode        ExportTextMode `json:"mode"`
	Replacement *string        `json:"replacement"`
}

type ExportChannelSelection struct {
	ChannelID string `json:"channel_id"`
	Label     string `json:"label"`
}

type ExportMessageSelection struct {
	ChannelID string           `json:"channel_id"`
	TS        string           `json:"ts"`
	Text      ExportTextChoice `json:"text"`
}

type ExportSelection struct {
	WorkspaceID    string                   `json:"workspace_id"`
	WorkspaceLabel string                   `json:"workspace_label"`
	Channels       []ExportChannelSelection `json:"channels"`
	Messages       []ExportMessageSelection `json:"messages"`
}

type ExportChannelBinding struct {
	ExportChannelSelection
	SourceSHA256 string `json:"source_sha256"`
}

type ExportMessageBinding struct {
	ExportMessageSelection
	SourceSHA256 string `json:"source_sha256"`
}

// ExportSelectionPlan is private input, suitable for JSON storage by a future
// caller. Its hashes detect source changes; they grant no publication authority.
type ExportSelectionPlan struct {
	Version         int                    `json:"version"`
	WorkspaceID     string                 `json:"workspace_id"`
	WorkspaceLabel  string                 `json:"workspace_label"`
	WorkspaceSHA256 string                 `json:"workspace_sha256"`
	Channels        []ExportChannelBinding `json:"channels"`
	Messages        []ExportMessageBinding `json:"messages"`
}

type ExportMessage struct {
	ChannelID string  `json:"channel_id"`
	TS        string  `json:"ts"`
	UserID    *string `json:"user_id"`
	Text      string  `json:"text"`
	ThreadTS  *string `json:"thread_ts"`
	EditedTS  *string `json:"edited_ts"`
}

// ExportProjection contains only explicit labels and selected message scalars.
// It does not certify conversation origin or the contents of kept/replaced text.
type ExportProjection struct {
	WorkspaceID    string                   `json:"workspace_id"`
	WorkspaceLabel string                   `json:"workspace_label"`
	Channels       []ExportChannelSelection `json:"channels"`
	Messages       []ExportMessage          `json:"messages"`
}

func PrepareExportSelection(ctx context.Context, archivePath string, selection ExportSelection) (ExportSelectionPlan, error) {
	plan, _, err := readExportSelection(ctx, archivePath, selection)
	return plan, err
}

func ResolveExportSelection(ctx context.Context, archivePath string, plan ExportSelectionPlan) (ExportProjection, error) {
	if plan.Version != ExportSelectionVersion {
		return ExportProjection{}, errors.New("unsupported export selection version")
	}
	selection := ExportSelection{WorkspaceID: plan.WorkspaceID, WorkspaceLabel: plan.WorkspaceLabel}
	for _, channel := range plan.Channels {
		selection.Channels = append(selection.Channels, channel.ExportChannelSelection)
	}
	for _, message := range plan.Messages {
		selection.Messages = append(selection.Messages, message.ExportMessageSelection)
	}
	current, projection, err := readExportSelection(ctx, archivePath, selection)
	if err != nil {
		return ExportProjection{}, err
	}
	if current.WorkspaceSHA256 != plan.WorkspaceSHA256 {
		return ExportProjection{}, errors.New("selected workspace binding changed")
	}
	channels := make(map[string]string, len(plan.Channels))
	for _, channel := range plan.Channels {
		channels[channel.ChannelID] = channel.SourceSHA256
	}
	for i, channel := range current.Channels {
		if channels[channel.ChannelID] != channel.SourceSHA256 {
			return ExportProjection{}, fmt.Errorf("selected channel %d binding changed", i)
		}
	}
	messages := make(map[selectionMessageKey]string, len(plan.Messages))
	for _, message := range plan.Messages {
		messages[selectionMessageKey{message.ChannelID, message.TS}] = message.SourceSHA256
	}
	for i, message := range current.Messages {
		if messages[selectionMessageKey{message.ChannelID, message.TS}] != message.SourceSHA256 {
			return ExportProjection{}, fmt.Errorf("selected message %d binding changed", i)
		}
	}
	return projection, nil
}

type selectionMessageKey struct{ channel, ts string }

// Byte slices preserve exact SQL TEXT bytes, including NULL versus empty TEXT,
// in deterministic JSON tuples. Raw bodies contribute only their SHA256.
type selectionWorkspaceRow struct {
	Version                                   int
	ID, Name, Domain, EnterpriseID, UpdatedAt []byte
	RawSHA256                                 string
}

type selectionChannelRow struct {
	Version                                                int
	ID, WorkspaceID, Name, Kind, Topic, Purpose, UpdatedAt []byte
	IsPrivate, IsArchived, IsShared, IsGeneral             int64
	RawSHA256                                              string
}

type selectionMessageRow struct {
	Version                                                        int
	ChannelID, TS, WorkspaceID, UserID, Subtype, ClientMsgID       []byte
	ThreadTS, ParentUserID, Text, LatestReply, EditedTS, DeletedTS []byte
	SourceName, UpdatedAt                                          []byte
	ReplyCount, SourceRank                                         int64
	RawSHA256                                                      string
}

func readExportSelection(ctx context.Context, archivePath string, selection ExportSelection) (ExportSelectionPlan, ExportProjection, error) {
	selection, err := canonicalExportSelection(selection)
	if err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, err
	}
	// This contract accepts files, not SQLite URI options or in-memory stores.
	if archivePath == "" || strings.TrimSpace(archivePath) != archivePath || archivePath == ":memory:" || strings.HasPrefix(archivePath, "file:") {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("export selection requires an existing archive file")
	}
	info, err := os.Stat(archivePath)
	if err != nil || !info.Mode().IsRegular() {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("export selection requires an existing regular archive file")
	}
	s, err := store.OpenReadOnly(archivePath)
	if err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("cannot open export selection archive read-only")
	}
	defer func() { _ = s.Close() }()
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("cannot start export selection snapshot")
	}
	defer func() { _ = tx.Rollback() }()
	plan, projection, err := selectExportRows(ctx, tx, selection)
	if err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("cannot finish export selection snapshot")
	}
	if err := s.Close(); err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("cannot close export selection archive")
	}
	return plan, projection, nil
}

func canonicalExportSelection(selection ExportSelection) (ExportSelection, error) {
	if strings.TrimSpace(selection.WorkspaceID) == "" || !utf8.ValidString(selection.WorkspaceID) || !utf8.ValidString(selection.WorkspaceLabel) {
		return ExportSelection{}, errors.New("export selection requires a valid workspace identity and label")
	}
	if len(selection.Channels) == 0 || len(selection.Messages) == 0 {
		return ExportSelection{}, errors.New("export selection requires explicit channels and messages")
	}
	if selection.WorkspaceLabel == "" {
		selection.WorkspaceLabel = selection.WorkspaceID
	}
	selection.Channels = slices.Clone(selection.Channels)
	selection.Messages = slices.Clone(selection.Messages)
	channels := make(map[string]bool, len(selection.Channels))
	for i := range selection.Channels {
		channel := &selection.Channels[i]
		if strings.TrimSpace(channel.ChannelID) == "" || !utf8.ValidString(channel.ChannelID) || !utf8.ValidString(channel.Label) || channels[channel.ChannelID] {
			return ExportSelection{}, fmt.Errorf("selected channel %d has invalid or duplicate identity/label", i)
		}
		channels[channel.ChannelID] = true
		if channel.Label == "" {
			channel.Label = channel.ChannelID
		}
	}
	messages := make(map[selectionMessageKey]bool, len(selection.Messages))
	for i := range selection.Messages {
		message := &selection.Messages[i]
		key := selectionMessageKey{message.ChannelID, message.TS}
		if !channels[message.ChannelID] || strings.TrimSpace(message.TS) == "" || !utf8.ValidString(message.TS) || messages[key] {
			return ExportSelection{}, fmt.Errorf("selected message %d has invalid, duplicate or unselected identity", i)
		}
		messages[key] = true
		switch message.Text.Mode {
		case ExportTextKeep:
			if message.Text.Replacement != nil {
				return ExportSelection{}, fmt.Errorf("selected message %d keep choice contains replacement text", i)
			}
		case ExportTextReplace:
			if message.Text.Replacement == nil || !utf8.ValidString(*message.Text.Replacement) {
				return ExportSelection{}, fmt.Errorf("selected message %d requires explicit valid replacement text", i)
			}
			replacement := *message.Text.Replacement
			message.Text.Replacement = &replacement
		default:
			return ExportSelection{}, fmt.Errorf("selected message %d requires keep or replace text choice", i)
		}
	}
	slices.SortFunc(selection.Channels, func(a, b ExportChannelSelection) int { return strings.Compare(a.ChannelID, b.ChannelID) })
	slices.SortFunc(selection.Messages, func(a, b ExportMessageSelection) int {
		if order := strings.Compare(a.ChannelID, b.ChannelID); order != 0 {
			return order
		}
		return strings.Compare(a.TS, b.TS)
	})
	return selection, nil
}

func selectExportRows(ctx context.Context, tx *sql.Tx, selection ExportSelection) (ExportSelectionPlan, ExportProjection, error) {
	plan := ExportSelectionPlan{Version: ExportSelectionVersion, WorkspaceID: selection.WorkspaceID, WorkspaceLabel: selection.WorkspaceLabel}
	projection := ExportProjection{WorkspaceID: selection.WorkspaceID, WorkspaceLabel: selection.WorkspaceLabel, Channels: selection.Channels, Messages: []ExportMessage{}}
	workspace := selectionWorkspaceRow{Version: ExportSelectionVersion}
	var raw string
	err := tx.QueryRowContext(ctx, `select id, name, domain, enterprise_id, updated_at, raw_json from workspaces where id = ?`, selection.WorkspaceID).
		Scan(&workspace.ID, &workspace.Name, &workspace.Domain, &workspace.EnterpriseID, &workspace.UpdatedAt, &raw)
	if err != nil {
		return ExportSelectionPlan{}, ExportProjection{}, errors.New("selected workspace is absent or unreadable")
	}
	workspace.RawSHA256 = selectionRawSHA256(raw)
	plan.WorkspaceSHA256 = selectionRowSHA256(workspace)
	for i, selected := range selection.Channels {
		channel := selectionChannelRow{Version: ExportSelectionVersion}
		err := tx.QueryRowContext(ctx, `select id, workspace_id, name, kind, topic, purpose, is_private, is_archived, is_shared, is_general, updated_at, raw_json from channels where id = ?`, selected.ChannelID).
			Scan(&channel.ID, &channel.WorkspaceID, &channel.Name, &channel.Kind, &channel.Topic, &channel.Purpose, &channel.IsPrivate, &channel.IsArchived, &channel.IsShared, &channel.IsGeneral, &channel.UpdatedAt, &raw)
		if err != nil {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected channel %d is absent or unreadable", i)
		}
		if string(channel.WorkspaceID) != selection.WorkspaceID {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected channel %d has different workspace ownership", i)
		}
		kind := string(channel.Kind)
		if channel.IsPrivate != 0 || (kind != "public_channel" && kind != "public" && kind != "desktop_channel") || !selectionNativePublic(raw, selected.ChannelID, selection.WorkspaceID) {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected channel %d lacks unambiguous current public-channel evidence", i)
		}
		channel.RawSHA256 = selectionRawSHA256(raw)
		plan.Channels = append(plan.Channels, ExportChannelBinding{ExportChannelSelection: selected, SourceSHA256: selectionRowSHA256(channel)})
	}
	rows := make(map[selectionMessageKey]selectionMessageRow, len(selection.Messages))
	for i, selected := range selection.Messages {
		message := selectionMessageRow{Version: ExportSelectionVersion}
		err := tx.QueryRowContext(ctx, `select channel_id, ts, workspace_id, user_id, subtype, client_msg_id, thread_ts, parent_user_id, text, reply_count, latest_reply, edited_ts, deleted_ts, source_rank, source_name, updated_at, raw_json from messages where channel_id = ? and ts = ?`, selected.ChannelID, selected.TS).
			Scan(&message.ChannelID, &message.TS, &message.WorkspaceID, &message.UserID, &message.Subtype, &message.ClientMsgID, &message.ThreadTS, &message.ParentUserID, &message.Text, &message.ReplyCount, &message.LatestReply, &message.EditedTS, &message.DeletedTS, &message.SourceRank, &message.SourceName, &message.UpdatedAt, &raw)
		if err != nil {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d is absent or unreadable", i)
		}
		if string(message.WorkspaceID) != selection.WorkspaceID {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d has different workspace ownership", i)
		}
		subtype := string(message.Subtype)
		if strings.HasPrefix(selected.TS, "draft:") || subtype == "desktop_draft" || subtype == "draft" || string(message.SourceName) == "desktop-draft" || len(message.DeletedTS) != 0 || subtype == "message_deleted" {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d is a draft or deleted", i)
		}
		if !utf8.Valid(message.UserID) || !utf8.Valid(message.ThreadTS) || !utf8.Valid(message.EditedTS) {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d contains invalid text encoding", i)
		}
		message.RawSHA256 = selectionRawSHA256(raw)
		rows[selectionMessageKey{selected.ChannelID, selected.TS}] = message
		plan.Messages = append(plan.Messages, ExportMessageBinding{ExportMessageSelection: selected, SourceSHA256: selectionRowSHA256(message)})
		text := string(message.Text)
		if selected.Text.Mode == ExportTextReplace {
			text = *selected.Text.Replacement
		}
		if !utf8.ValidString(text) {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d contains invalid text encoding", i)
		}
		projection.Messages = append(projection.Messages, ExportMessage{ChannelID: selected.ChannelID, TS: selected.TS, UserID: selectionNullableText(message.UserID), Text: text, ThreadTS: selectionNullableText(message.ThreadTS), EditedTS: selectionNullableText(message.EditedTS)})
	}
	for i, selected := range selection.Messages {
		message := rows[selectionMessageKey{selected.ChannelID, selected.TS}]
		parentTS := string(message.ThreadTS)
		if parentTS == "" || parentTS == selected.TS {
			continue
		}
		parent, ok := rows[selectionMessageKey{selected.ChannelID, parentTS}]
		if !ok || (len(parent.ThreadTS) != 0 && string(parent.ThreadTS) != parentTS) {
			return ExportSelectionPlan{}, ExportProjection{}, fmt.Errorf("selected message %d requires its selected eligible root in the same channel", i)
		}
	}
	return plan, projection, nil
}

func selectionNullableText(value []byte) *string {
	if value == nil {
		return nil
	}
	text := string(value)
	return &text
}

func selectionRowSHA256[T selectionWorkspaceRow | selectionChannelRow | selectionMessageRow](row T) string {
	// Only the three private, scalar-only tuple types above reach this encoder.
	body, _ := json.Marshal(row)
	return selectionRawSHA256(string(body))
}

func selectionRawSHA256(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func selectionNativePublic(raw, channelID, workspaceID string) bool {
	if !utf8.ValidString(raw) {
		return false
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok {
			return false
		}
		for _, recognized := range []string{"id", "channel_id", "workspace_id", "team_id", "context_team_id", "is_channel", "is_private", "is_group", "is_im", "is_mpim"} {
			if strings.EqualFold(key, recognized) {
				key = recognized
				break
			}
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
		switch key {
		case "id", "channel_id", "workspace_id", "team_id", "context_team_id", "is_channel", "is_private", "is_group", "is_im", "is_mpim":
			// Case-varied or escaped duplicate fields must not overwrite a veto.
			if seen[key] {
				return false
			}
			seen[key] = true
		default:
			continue
		}
		switch key {
		case "is_channel":
			if string(value) != "true" {
				return false
			}
		case "is_private", "is_group", "is_im", "is_mpim":
			if string(value) != "false" {
				return false
			}
		default:
			var identity string
			if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &identity) != nil {
				return false
			}
			if key == "id" || key == "channel_id" {
				if identity != channelID {
					return false
				}
			} else if identity != "" && identity != workspaceID {
				return false
			}
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return false
	}
	return seen["id"] && seen["is_channel"] && seen["is_private"] && seen["is_group"] && seen["is_im"] && seen["is_mpim"]
}
