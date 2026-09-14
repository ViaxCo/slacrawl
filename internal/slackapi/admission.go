package slackapi

import (
	"errors"
	"fmt"
	"strings"

	"github.com/slack-go/slack"

	"github.com/openclaw/slacrawl/internal/admission"
)

func conversationKind(channel slack.Channel) admission.Kind {
	return (admission.NativeFlags{
		IsIM: channel.IsIM, IsMPIM: channel.IsMpIM,
		IsChannel: channel.IsChannel, IsGroup: channel.IsGroup, IsPrivate: channel.IsPrivate,
	}).Kind()
}

func (c *Client) admitChannels(workspaceID string, channels []slack.Channel, skips *threadSkipTracker) ([]slack.Channel, error) {
	admitted := make([]slack.Channel, 0, len(channels))
	for _, channel := range channels {
		if channel.ID == "" {
			return nil, errors.New("conversation is missing a channel ID")
		}
		if channel.ContextTeamID != "" && channel.ContextTeamID != workspaceID {
			return nil, fmt.Errorf("channel %s context workspace does not match authenticated workspace", channel.ID)
		}
		kind := conversationKind(channel)
		if !c.dmPolicy.Allows(kind) {
			if kind == admission.Unknown {
				return nil, fmt.Errorf("channel %s has unknown or conflicting conversation type with include_dms=false", channel.ID)
			}
			skips.RecordOmission()
			continue
		}
		if channel.Latest != nil {
			if err := validateMessageChannel(*channel.Latest, channel.ID); err != nil {
				return nil, fmt.Errorf("channel %s latest: %w", channel.ID, err)
			}
		}
		admitted = append(admitted, channel)
	}
	return admitted, nil
}

func validateMessageChannel(message slack.Message, channelID string) error {
	for _, part := range []*slack.Msg{&message.Msg, message.SubMessage, message.PreviousMessage, message.Root} {
		if part != nil && part.Channel != "" && part.Channel != channelID {
			return errors.New("message channel does not match requested conversation")
		}
	}
	return nil
}

func validateMessagePage(messages []rawConversationMessage, channelID string) error {
	// Preflight the whole page before converting or writing any record. Otherwise
	// an earlier message or thread request could persist a rejected page's data.
	for _, message := range messages {
		if err := validateMessageChannel(message.Message, channelID); err != nil {
			return err
		}
		if strings.TrimSpace(message.Message.Timestamp) == "" {
			return errors.New("message is missing a timestamp")
		}
	}
	return nil
}
