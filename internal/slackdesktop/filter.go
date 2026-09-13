package slackdesktop

import (
	"strings"
)

func newIngestFilter(opts IngestOptions) ingestFilter {
	excludeChannels, excludeSelectors := channelSelectorSet(opts.ExcludeChannels)
	return ingestFilter{
		workspaceID:      strings.TrimSpace(opts.WorkspaceID),
		channels:         stringSet(opts.Channels),
		excludeChannels:  excludeChannels,
		excludeSelectors: excludeSelectors,
	}
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	return set
}

func channelSelectorSet(values []string) (map[string]struct{}, []channelSelector) {
	if len(values) == 0 {
		return nil, nil
	}
	set := make(map[string]struct{}, len(values))
	selectors := make([]channelSelector, 0, len(values))
	for _, value := range values {
		raw := strings.TrimSpace(value)
		selectorValue := raw
		explicitID := false
		if strings.HasPrefix(strings.ToLower(selectorValue), "id:") {
			explicitID = true
			selectorValue = strings.TrimSpace(selectorValue[len("id:"):])
		}
		selector := normalizeChannelSelector(selectorValue)
		if selector != "" {
			set[selector] = struct{}{}
			selectors = append(selectors, channelSelector{raw: raw, normalized: selector, explicitID: explicitID})
		}
	}
	return set, selectors
}

func normalizeChannelSelector(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "#"))
}

func (f ingestFilter) allowWorkspace(workspaceID string) bool {
	workspaceID = strings.TrimSpace(workspaceID)
	if f.workspaceID == "" {
		return true
	}
	return workspaceID == f.workspaceID
}

func (f ingestFilter) allowChannelNames(workspaceID, channelID string, channelNames []string) bool {
	channelID = strings.TrimSpace(channelID)
	if !f.allowWorkspace(workspaceID) {
		return false
	}
	if f.excludesChannel(channelID, channelNames) {
		return false
	}
	if len(f.channels) == 0 {
		return true
	}
	if channelID == "" {
		return false
	}
	_, allowed := f.channels[channelID]
	return allowed
}

func (f ingestFilter) excludesChannel(channelID string, channelNames []string) bool {
	if len(f.excludeChannels) == 0 {
		return false
	}
	if _, excluded := f.excludeChannels[normalizeChannelSelector(channelID)]; excluded {
		return true
	}
	if len(channelNames) == 0 {
		return false
	}
	sawName := false
	for _, channelName := range channelNames {
		channelName = normalizeChannelSelector(channelName)
		if channelName == "" {
			continue
		}
		sawName = true
		if _, excluded := f.excludeChannels[channelName]; excluded {
			return true
		}
	}
	if !sawName {
		return false
	}
	return false
}

func (f *ingestFilter) resolveKnownChannelIDs(channelIDs map[string]struct{}) {
	f.hasNameExclude = false
}

func desktopChannelIDs(extracted ExtractedData) map[string]struct{} {
	ids := map[string]struct{}{}
	add := func(channelID string) {
		channelID = normalizeChannelSelector(channelID)
		if channelID != "" {
			ids[channelID] = struct{}{}
		}
	}
	for _, channelIDs := range extracted.Recent {
		for _, channelID := range channelIDs {
			add(channelID)
		}
	}
	for _, marker := range extracted.ReadMarkers {
		add(marker.ChannelID)
	}
	for _, draft := range extracted.Drafts {
		for _, destination := range draft.Destinations {
			add(destination.ChannelID)
		}
	}
	for _, state := range extracted.ReduxStates {
		for _, channel := range state.Channels {
			add(channel.ID)
		}
		for _, message := range state.Messages {
			add(message.Channel)
		}
	}
	return ids
}

func (f ingestFilter) workspaceIDs(ids []string) []string {
	if f.workspaceID == "" {
		return ids
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if f.allowWorkspace(id) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

type channelNameHints map[string][]string

func channelNamesByWorkspaceID(states []ReduxDecodedState) channelNameHints {
	names := channelNameHints{}
	for _, state := range states {
		memberNames := map[string]string{}
		for _, member := range state.Members {
			memberNames[member.ID] = firstNonEmpty(member.Profile.DisplayName, member.Profile.RealName, member.Name, member.Real, member.ID)
		}
		for _, channel := range state.Channels {
			workspaceID := channelContextWorkspaceID(channel.ContextTeamID, state.WorkspaceID)
			key := channelNameKey(workspaceID, channel.ID)
			if channel.ID == "" {
				continue
			}
			aliases := reduxResolvedChannelNames(channel, memberNames)
			if len(aliases) > 0 {
				names[key] = uniqueNonEmptyStrings(append(names[key], aliases...))
			} else if _, exists := names[key]; !exists {
				names[key] = nil
			}
		}
	}
	return names
}

func (h channelNameHints) get(workspaceID, channelID string) []string {
	return h[channelNameKey(workspaceID, channelID)]
}

func channelNameKey(workspaceID, channelID string) string {
	return strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(channelID)
}
