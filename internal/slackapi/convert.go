package slackapi

import (
	"time"

	"github.com/slack-go/slack"

	"github.com/openclaw/slacrawl/internal/search"
	"github.com/openclaw/slacrawl/internal/store"
)

func toStoreChannel(workspaceID string, channel slack.Channel, now time.Time) store.Channel {
	kind := "public_channel"
	switch dmChannelKind(channel) {
	case "im", "mpim":
		kind = dmChannelKind(channel)
	case "":
		if channel.IsPrivate {
			kind = "private_channel"
		}
	}
	return store.Channel{
		ID:          channel.ID,
		WorkspaceID: workspaceID,
		Name:        channel.Name,
		Kind:        kind,
		Topic:       channel.Topic.Value,
		Purpose:     channel.Purpose.Value,
		IsPrivate:   channel.IsPrivate,
		IsArchived:  channel.IsArchived,
		IsShared:    channel.IsShared,
		IsGeneral:   channel.IsGeneral,
		RawJSON:     store.MarshalRaw(channel),
		UpdatedAt:   now,
	}
}

// ToStoreUser is the single slack.User -> store.User mapping; the export
// importer reuses it so a new stored field cannot silently miss one path.
func ToStoreUser(workspaceID string, user slack.User, now time.Time) store.User {
	return store.User{
		ID:          user.ID,
		WorkspaceID: workspaceID,
		Name:        user.Name,
		RealName:    user.RealName,
		DisplayName: user.Profile.DisplayName,
		Title:       user.Profile.Title,
		IsBot:       user.IsBot,
		IsDeleted:   user.Deleted,
		RawJSON:     store.MarshalRaw(user),
		UpdatedAt:   now,
	}
}

func toStoreMessage(workspaceID string, msg slack.Message, sourceName string, sourceRank int, rawPayload any, now time.Time) store.Message {
	editedTS := ""
	if msg.Edited != nil {
		editedTS = msg.Edited.Timestamp
	}
	normalizedText := search.NormalizeMessage(msg)
	if rawPayload != nil {
		normalizedText = search.NormalizeMessageWithRawPayload(msg, rawPayload)
	}
	return store.Message{
		ChannelID:      msg.Channel,
		TS:             msg.Timestamp,
		WorkspaceID:    workspaceID,
		UserID:         msg.User,
		Subtype:        msg.SubType,
		ClientMsgID:    msg.ClientMsgID,
		ThreadTS:       msg.ThreadTimestamp,
		ParentUserID:   msg.ParentUserId,
		Text:           msg.Text,
		NormalizedText: normalizedText,
		ReplyCount:     msg.ReplyCount,
		LatestReply:    msg.LatestReply,
		EditedTS:       editedTS,
		DeletedTS:      msg.DeletedTimestamp,
		SourceRank:     sourceRank,
		SourceName:     sourceName,
		RawJSON:        store.MarshalRaw(msg),
		UpdatedAt:      now,
		Files:          toStoreFiles(workspaceID, msg, now),
	}
}

func toStoreFiles(workspaceID string, msg slack.Message, now time.Time) []store.MessageFile {
	files := make([]store.MessageFile, 0, len(msg.Files))
	for _, file := range msg.Files {
		if file.ID == "" {
			continue
		}
		files = append(files, store.MessageFile{
			WorkspaceID:        workspaceID,
			ChannelID:          msg.Channel,
			TS:                 msg.Timestamp,
			FileID:             file.ID,
			UserID:             firstNonEmpty(file.User, msg.User),
			Name:               file.Name,
			Title:              file.Title,
			Mimetype:           file.Mimetype,
			Filetype:           file.Filetype,
			PrettyType:         file.PrettyType,
			Mode:               file.Mode,
			Size:               int64(file.Size),
			URLPrivate:         file.URLPrivate,
			URLPrivateDownload: file.URLPrivateDownload,
			Permalink:          file.Permalink,
			IsPublic:           file.IsPublic,
			PlainText:          file.PlainText,
			PreviewPlainText:   file.PreviewPlainText,
			RawJSON:            store.MarshalRaw(file),
			UpdatedAt:          now,
		})
	}
	return files
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func toStoreMentions(msg slack.Message) []store.Mention {
	raw := search.ExtractMentions(msg.Text)
	mentions := make([]store.Mention, 0, len(raw))
	for _, mention := range raw {
		mentions = append(mentions, store.Mention{
			Type:        mention.Type,
			TargetID:    mention.TargetID,
			DisplayText: mention.DisplayText,
		})
	}
	return mentions
}
