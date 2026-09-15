package slackapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
)

func (c *Client) admitTailEvent(ctx context.Context, st *store.Store, workspaceID string, event slackevents.EventsAPIEvent) (bool, error) {
	var channelID, channelType string
	var message *slackevents.MessageEvent
	switch ev := event.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		channelID, channelType, message = ev.Channel, ev.ChannelType, ev
	case *slackevents.ChannelRenameEvent:
		channelID = ev.Channel.ID
	case *slackevents.ChannelArchiveEvent:
		channelID = ev.Channel
	case *slackevents.ChannelUnarchiveEvent:
		channelID = ev.Channel
	default:
		return false, nil
	}
	if channelID == "" {
		return false, errors.New("tail event is missing a channel ID")
	}
	if message == nil {
		// Metadata events only update existing rows. Avoid looking up unrelated
		// conversations; the SQL mutation also checks ownership at write time.
		owner, err := st.ChannelWorkspaceID(ctx, channelID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if owner != workspaceID {
			return false, nil
		}
	}
	if c.dmPolicy == admission.Exclude {
		allowed, err := c.admitTailConversation(ctx, workspaceID, channelID, channelType)
		if err != nil {
			return false, err
		}
		if !allowed {
			c.warnLogger().Debug("skipping excluded DM event", "workspace_id", workspaceID, "channel_id", channelID)
			return false, nil
		}
	}
	if message != nil {
		// Validate before messageFromEvent can replace the envelope's identity
		// with an edited/deleted message or before raw blocks are inspected.
		if err := validateMessageChannel(slack.Message{
			SubMessage: message.Message, PreviousMessage: message.PreviousMessage, Root: message.Root,
		}, channelID); err != nil {
			return false, fmt.Errorf("tail channel %s: %w", channelID, err)
		}
	}
	return true, nil
}

func (c *Client) admitTailConversation(ctx context.Context, workspaceID, channelID, channelType string) (bool, error) {
	switch channelType {
	case slackevents.ChannelTypeChannel, slackevents.ChannelTypeGroup:
		return true, nil
	case slackevents.ChannelTypeIM, slackevents.ChannelTypeMPIM, "app_home":
		// Slack moved the retired workspace-app 1:1 message.im events to app_home.
		return false, nil
	}
	if c.bot == nil {
		return false, errors.New("bot token is required to classify an untyped tail event")
	}
	// Edits/deletions can omit channel_type. Resolve each untyped event afresh;
	// a stored kind or an earlier event is not proof of its current type.
	channel, err := c.getConversationInfo(ctx, channelID)
	if err != nil {
		return false, fmt.Errorf("classify tail channel %s: %w", channelID, err)
	}
	if channel.ID != channelID {
		return false, errors.New("tail conversation lookup returned a different channel ID")
	}
	if channel.ContextTeamID != "" && channel.ContextTeamID != workspaceID {
		return false, errors.New("tail conversation context workspace does not match authenticated workspace")
	}
	kind := conversationKind(*channel)
	if kind == admission.Unknown {
		return false, errors.New("tail conversation has unknown or conflicting type with include_dms=false")
	}
	// The lookup can contain a latest message. Retain only the admission result,
	// never the returned conversation or its nested content.
	return c.dmPolicy.Allows(kind), nil
}
