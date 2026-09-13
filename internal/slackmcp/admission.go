package slackmcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/openclaw/slacrawl/internal/admission"
)

type channelSelection struct {
	rows         []ChannelRecord
	observations []ChannelRecord
}

func resolveAdmissionChannels(ctx context.Context, client *Client, tools toolset, opts Options) (channelSelection, error) {
	var selection channelSelection
	selected := map[string]bool{}
	strict := opts.DMPolicy == admission.Exclude
	if strict || len(opts.Channels) == 0 {
		var catalog []ChannelRecord
		var err error
		if strict {
			catalog, err = client.referenceChannels(ctx, tools, true)
		} else {
			catalog, err = client.channels(ctx, tools, "")
		}
		if err != nil {
			return channelSelection{}, err
		}
		selection.observations = catalog
		if len(opts.Channels) == 0 {
			selection.rows = catalog
			for _, channel := range catalog {
				selected[channel.ID] = true
			}
		} else {
			for _, selector := range opts.Channels {
				selector = strings.TrimSpace(strings.TrimPrefix(selector, "#"))
				if selector == "" {
					continue
				}
				found := false
				for _, channel := range catalog {
					if matchesChannelSelector(channel, selector) {
						selected[channel.ID], found = true, true
					}
				}
				if !found {
					return channelSelection{}, errors.New("MCP conversation selector is absent from the fresh native catalog; check server channel access and configuration")
				}
			}
			selection.rows = catalog
		}
	} else {
		// Preserve legacy requests and first selected payloads, including ID stubs.
		// Keep all fetched evidence until later selectors have chosen their IDs.
		for _, selector := range opts.Channels {
			selector = strings.TrimSpace(strings.TrimPrefix(selector, "#"))
			if selector == "" {
				continue
			}
			if channelIDRE.MatchString(selector) {
				channel := ChannelRecord{ID: selector}
				selection.observations = append(selection.observations, channel)
				if !selected[selector] {
					selected[selector] = true
					selection.rows = append(selection.rows, channel)
				}
				continue
			}
			catalog, err := client.channels(ctx, tools, selector)
			if err != nil {
				return channelSelection{}, err
			}
			selection.observations = append(selection.observations, catalog...)
			found := false
			for _, channel := range catalog {
				if !matchesChannelSelector(channel, selector) {
					continue
				}
				found = true
				if !selected[channel.ID] {
					selected[channel.ID] = true
					selection.rows = append(selection.rows, channel)
				}
			}
			if !found {
				return channelSelection{}, fmt.Errorf("MCP channel %q not found", selector)
			}
		}
	}
	// Excluding one returned alias excludes its identity under every policy.
	// Filter only after selection, so unrelated catalog identities cannot fail it.
	exclusions := map[string]bool{}
	for _, value := range opts.ExcludeChannels {
		exclusions[strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "#"))] = true
	}
	excludedIDs := map[string]bool{}
	for _, channel := range selection.observations {
		if exclusions[strings.ToLower(channel.ID)] || exclusions[strings.ToLower(channel.Name)] {
			excludedIDs[channel.ID] = true
		}
	}
	retain := func(channels []ChannelRecord) []ChannelRecord {
		retained := make([]ChannelRecord, 0, len(channels))
		for _, channel := range channels {
			if selected[channel.ID] && !excludedIDs[channel.ID] {
				retained = append(retained, channel)
			}
		}
		return retained
	}
	selection.rows = retain(selection.rows)
	selection.observations = retain(selection.observations)
	return selection, nil
}

func matchesChannelSelector(channel ChannelRecord, selector string) bool {
	if channelIDRE.MatchString(selector) {
		return channel.ID == selector
	}
	return strings.EqualFold(channel.ID, selector) || strings.EqualFold(channel.Name, selector)
}

func admitMCPChannels(selection channelSelection, workspaceID string, policy admission.DMPolicy) ([]ChannelRecord, int, error) {
	kinds := map[string]uint8{}
	for _, channel := range selection.observations {
		if strings.TrimSpace(channel.ID) == "" {
			return nil, 0, errors.New("MCP conversation is missing an ID")
		}
		kind := admission.Unknown
		if channel.native != nil {
			// Bind every selected observation before its type can veto another row.
			// A foreign workspace must not hide this workspace's conversation as a DM.
			if channel.native.ContextTeamID != "" && channel.native.ContextTeamID != workspaceID {
				return nil, 0, errors.New("MCP conversation context workspace does not match the configured workspace")
			}
			kind = channel.native.NativeFlags.Kind()
		}
		kinds[channel.ID] |= 1 << kind
	}
	const dmKinds = (1 << admission.IM) | (1 << admission.MPIM)
	for _, channel := range selection.observations {
		if policy == admission.Exclude {
			observed := kinds[channel.ID]
			if observed&dmKinds != 0 {
				continue
			}
			if observed != 1<<admission.PublicChannel && observed != 1<<admission.PrivateChannel {
				return nil, 0, errors.New("MCP conversation has unknown or conflicting native type with include_dms=false")
			}
		}
		if channel.native != nil && channel.native.Latest != nil {
			if err := validateReferenceMessage(*channel.native.Latest, workspaceID, channel.ID, ""); err != nil {
				return nil, 0, err
			}
		}
	}
	admitted := make([]ChannelRecord, 0, len(selection.rows))
	seen := map[string]bool{}
	omitted := 0
	for _, channel := range selection.rows {
		if policy == admission.Exclude {
			if seen[channel.ID] {
				continue
			}
			seen[channel.ID] = true
			if kinds[channel.ID]&dmKinds != 0 {
				omitted++
				continue
			}
		}
		admitted = append(admitted, channel)
	}
	return admitted, omitted, nil
}

func validateReferenceMessages(messages []referenceMessage, workspaceID, channelID, threadTS string) error {
	// Validate before timestamp filtering and conversion discard returned facts.
	// A rejected reply set must never update its parent or any earlier reply.
	for _, message := range messages {
		if err := validateReferenceMessage(message, workspaceID, channelID, threadTS); err != nil {
			return err
		}
		if strings.TrimSpace(message.TS) == "" {
			return errors.New("MCP message timestamp is empty")
		}
	}
	return nil
}

func validateReferenceMessage(message referenceMessage, workspaceID, channelID, threadTS string) error {
	if message.Channel != "" && message.Channel != channelID {
		return errors.New("MCP message channel does not match requested conversation")
	}
	if message.ContextTeamID != "" && message.ContextTeamID != workspaceID {
		return errors.New("MCP message context workspace does not match the configured workspace")
	}
	if threadTS != "" && message.ThreadTS != "" && message.ThreadTS != threadTS {
		return errors.New("MCP reply thread does not match requested thread")
	}
	for _, nested := range []*referenceMessage{message.SubMessage, message.PreviousMessage, message.Root} {
		if nested != nil {
			if err := validateReferenceMessage(*nested, workspaceID, channelID, threadTS); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateThreadPage(page threadPage, channelID, threadTS string) error {
	if page.Parent != nil && (page.Parent.ChannelID != channelID || page.Parent.TS != threadTS) {
		return errors.New("MCP thread parent does not match requested conversation and timestamp")
	}
	for _, reply := range page.Replies {
		if reply.ChannelID != channelID || reply.ThreadTS != threadTS {
			return errors.New("MCP reply does not match requested conversation and thread")
		}
		if strings.TrimSpace(reply.TS) == "" || reply.TS == threadTS {
			return errors.New("MCP reply timestamp is empty or matches its parent")
		}
	}
	return nil
}
