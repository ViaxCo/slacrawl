package slackdesktop

import (
	"errors"
	"strings"

	"github.com/openclaw/slacrawl/internal/admission"
)

// Decoder evidence is ephemeral: channel/message payloads retain their original
// JSON shape, while admission sees identities discarded by legacy selection.
type reduxProvenance struct {
	Channels     []reduxChannelEvidence    `json:"channels"`
	Messages     []reduxMessageEvidence    `json:"messages"`
	Observations []reduxChannelObservation `json:"observations"`
}

type reduxChannelEvidence struct {
	Key     string   `json:"key"`
	Aliases []string `json:"aliases"`
}

type reduxChannelObservation struct {
	ReduxChannel
	Key     string   `json:"key"`
	Aliases []string `json:"aliases"`
}

type reduxMessageEvidence struct {
	Channel    string   `json:"channel"`
	TS         string   `json:"ts"`
	Aliases    []string `json:"aliases"`
	Containers []string `json:"containers"`
	Conflict   bool     `json:"conflict"`
	Supported  bool     `json:"supported"`
}

type kindObservations uint8

func (k kindObservations) reason() string {
	if k&((1<<admission.IM)|(1<<admission.MPIM)) != 0 {
		return "dm"
	}
	if k == 1<<admission.PublicChannel || k == 1<<admission.PrivateChannel {
		return ""
	}
	return "unknown_conversation"
}

func desktopConversationKind(channel ReduxChannel) admission.Kind {
	return (admission.NativeFlags{
		IsIM: channel.IsIM, IsMPIM: channel.IsMPIM,
		IsChannel: channel.IsChannel, IsGroup: channel.IsGroup, IsPrivate: channel.IsPrivate,
	}).Kind()
}

func observeReduxKinds(observations map[string]kindObservations, state ReduxDecodedState) {
	for _, observation := range state.Provenance.Observations {
		channel := observation.ReduxChannel
		workspaceID := channelContextWorkspaceID(channel.ContextTeamID, state.WorkspaceID)
		ids := append([]string{observation.Key, channel.ID}, observation.Aliases...)
		kind := desktopConversationKind(channel)
		if channel.ID == "" || identityConflict(channel.ID, ids...) {
			// A map key can identify negative evidence, but sparse/mismatched
			// metadata must never supply positive admission classification.
			if kind != admission.IM && kind != admission.MPIM {
				kind = admission.Unknown
			}
		}
		for _, id := range ids {
			if id != "" {
				observations[channelNameKey(workspaceID, id)] |= 1 << kind
			}
		}
	}
}

// Counts describe intake omissions, not deleted archive rows or historical
// privacy guarantees. Fixed fields prevent payloads or IDs becoming diagnostics.
type AdmissionSummary struct {
	DM                   int `json:"dm"`
	UnknownConversation  int `json:"unknown_conversation"`
	UnsupportedMessage   int `json:"unsupported_message"`
	AmbiguousWorkspace   int `json:"ambiguous_workspace"`
	OutOfScope           int `json:"out_of_scope"`
	UnattributedMetadata int `json:"unattributed_metadata"`
	DecoderUnavailable   int `json:"decoder_unavailable"`
	DecodeFailures       int `json:"decode_failures"`
	Channels             int `json:"channels"`
	Messages             int `json:"messages"`
}

func (s *AdmissionSummary) omit(reason string) bool {
	switch reason {
	case "dm":
		s.DM++
	case "unknown_conversation":
		s.UnknownConversation++
	case "unsupported_message":
		s.UnsupportedMessage++
	case "ambiguous_workspace":
		s.AmbiguousWorkspace++
	case "out_of_scope":
		s.OutOfScope++
	}
	return false
}

type ownedRecord[T any] struct {
	value       T
	workspaceID string
}

type recentChannel struct{ channelID, persistWorkspaceID string }

type preparedReduxState struct {
	workspaceID string
	channels    []ownedRecord[ReduxChannel]
	messages    []ownedRecord[ReduxMessage]
	members     []ReduxMember
	memberNames map[string]string
}

type preparedDesktop struct {
	data    ExtractedData
	redux   []preparedReduxState
	drafts  []ownedRecord[Draft]
	recent  []ownedRecord[recentChannel]
	markers []ownedRecord[ReadMarker]
	summary AdmissionSummary
}

type desktopAdmission struct {
	filter     ingestFilter
	policy     admission.DMPolicy
	names      channelNameHints
	candidates map[string]map[string]struct{}
	selected   map[string]kindObservations
	observed   map[string]kindObservations
	summary    *AdmissionSummary
}

func newDesktopAdmission(states []ReduxDecodedState, filter ingestFilter, policy admission.DMPolicy, summary *AdmissionSummary) desktopAdmission {
	a := desktopAdmission{filter: filter, policy: policy, names: channelNamesByWorkspaceID(states), candidates: channelWorkspaceCandidates(states), selected: map[string]kindObservations{}, observed: map[string]kindObservations{}, summary: summary}
	for _, state := range states {
		for _, channel := range state.Channels {
			a.selected[channelNameKey(channelContextWorkspaceID(channel.ContextTeamID, state.WorkspaceID), channel.ID)] |= 1 << desktopConversationKind(channel)
		}
		for key, kinds := range state.observations {
			a.observed[key] |= kinds
		}
	}
	return a
}

func (a desktopAdmission) allowSelected(workspaceID, channelID string) bool {
	if !a.filter.allowChannelNames(workspaceID, channelID, a.names.get(workspaceID, channelID)) {
		return a.summary.omit("out_of_scope")
	}
	if a.policy != admission.Exclude {
		return true
	}
	key := channelNameKey(workspaceID, channelID)
	// Historical observations only veto; selected metadata must itself prove type.
	if reason := a.selected[key].reason(); reason != "" {
		return a.summary.omit(reason)
	}
	return true
}

func (a desktopAdmission) allowObserved(workspaceID, channelID string) bool {
	if a.policy != admission.Exclude {
		return true
	}
	key := channelNameKey(workspaceID, channelID)
	if reason := (a.selected[key] | a.observed[key]).reason(); reason != "" {
		return a.summary.omit(reason)
	}
	return true
}

func (a desktopAdmission) allow(workspaceID, channelID string) bool {
	return a.allowSelected(workspaceID, channelID) && a.allowObserved(workspaceID, channelID)
}

func (a desktopAdmission) resolve(channelID, fallbackWorkspace string) (string, bool) {
	workspaceID, ok := resolveChannelWorkspace(channelID, fallbackWorkspace, a.candidates)
	if !ok {
		return "", a.summary.omit("ambiguous_workspace")
	}
	return workspaceID, a.allow(workspaceID, channelID)
}

func identityConflict(expected string, ids ...string) bool {
	for _, id := range ids {
		if id != "" && id != expected {
			return true
		}
	}
	return false
}

func (a desktopAdmission) prepareRedux(states []ReduxDecodedState) ([]preparedReduxState, error) {
	prepared := make([]preparedReduxState, 0, len(states))
	for _, state := range states {
		if state.WorkspaceID == "" {
			continue
		}
		out := preparedReduxState{workspaceID: state.WorkspaceID, memberNames: map[string]string{}}
		for _, member := range state.Members {
			out.memberNames[member.ID] = firstNonEmpty(member.Profile.DisplayName, member.Profile.RealName, member.Name, member.Real, member.ID)
		}
		localOwners := map[string]string{}
		referencedUsers := map[string]struct{}{}
		for _, channel := range state.Channels {
			localOwners[channel.ID] = channelContextWorkspaceID(channel.ContextTeamID, state.WorkspaceID)
		}
		for i, channel := range state.Channels {
			workspaceID := channelContextWorkspaceID(channel.ContextTeamID, state.WorkspaceID)
			if !a.allowSelected(workspaceID, channel.ID) {
				continue
			}
			if i < len(state.Provenance.Channels) {
				evidence := state.Provenance.Channels[i]
				if identityConflict(channel.ID, evidence.Key) || identityConflict(channel.ID, evidence.Aliases...) {
					return nil, errors.New("desktop channel identity conflicts with retained cache container")
				}
			}
			if !a.allowObserved(workspaceID, channel.ID) {
				continue
			}
			out.channels = append(out.channels, ownedRecord[ReduxChannel]{channel, workspaceID})
			referencedUsers[channel.User] = struct{}{}
			for _, userID := range channel.Members {
				referencedUsers[userID] = struct{}{}
			}
			a.summary.Channels++
		}
		for i, message := range state.Messages {
			if message.Channel == "" || message.TS == "" {
				continue
			}
			workspaceID, known := localOwners[message.Channel]
			if !known {
				if !isSlackConversationID(message.Channel) {
					continue
				}
				var ok bool
				workspaceID, ok = resolveChannelWorkspace(message.Channel, state.WorkspaceID, a.candidates)
				if !ok {
					a.summary.omit("ambiguous_workspace")
					continue
				}
			}
			if !a.allowSelected(workspaceID, message.Channel) {
				continue
			}
			evidence := reduxMessageEvidence{}
			if i < len(state.Provenance.Messages) {
				evidence = state.Provenance.Messages[i]
			}
			if evidence.Conflict || identityConflict(message.Channel, evidence.Aliases...) || identityConflict(message.Channel, evidence.Containers...) {
				return nil, errors.New("desktop message identity conflicts with retained cache container")
			}
			if !a.allowObserved(workspaceID, message.Channel) {
				continue
			}
			if a.policy == admission.Exclude && !evidence.Supported {
				a.summary.omit("unsupported_message")
				continue
			}
			if strings.TrimSpace(message.Text) == "" && message.Subtype == "" && message.Type == "" {
				continue
			}
			out.messages = append(out.messages, ownedRecord[ReduxMessage]{message, workspaceID})
			referencedUsers[message.User] = struct{}{}
			referencedUsers[message.ParentUserID] = struct{}{}
			a.summary.Messages++
		}
		for _, member := range state.Members {
			_, referenced := referencedUsers[member.ID]
			if a.filter.allowWorkspace(fallback(member.TeamID, state.WorkspaceID)) || referenced {
				out.members = append(out.members, member)
			}
		}
		prepared = append(prepared, out)
	}
	return prepared, nil
}

func prepareDesktop(extracted ExtractedData, opts IngestOptions) (preparedDesktop, error) {
	p := preparedDesktop{data: extracted}
	filter := newIngestFilter(opts)
	// Freeze names and workspace candidates before removing any records. Otherwise
	// filtering could turn an ambiguous channel into a different, unique owner.
	a := newDesktopAdmission(extracted.ReduxStates, filter, opts.DMPolicy, &p.summary)
	var err error
	p.redux, err = a.prepareRedux(extracted.ReduxStates)
	if err != nil {
		return preparedDesktop{}, err
	}
	p.data.Drafts = nil
	p.data.Recent = map[string][]string{}
	p.data.ReadMarkers = nil
	for workspaceID, channels := range extracted.Recent {
		for _, channelID := range channels {
			owner, ok := a.resolve(channelID, workspaceID)
			if !ok {
				continue
			}
			p.recent = append(p.recent, ownedRecord[recentChannel]{recentChannel{channelID, workspaceID}, owner})
			p.data.Recent[owner] = append(p.data.Recent[owner], channelID)
		}
	}
	for _, marker := range extracted.ReadMarkers {
		owner, ok := a.resolve(marker.ChannelID, marker.WorkspaceID)
		if !ok {
			continue
		}
		p.markers = append(p.markers, ownedRecord[ReadMarker]{marker, owner})
		p.data.ReadMarkers = append(p.data.ReadMarkers, marker)
	}
	for _, draft := range extracted.Drafts {
		if len(draft.Destinations) == 0 {
			continue
		}
		channelID := draft.Destinations[0].ChannelID
		workspaceID := draft.WorkspaceID
		if opts.DMPolicy != admission.Exclude {
			if workspaceID == "" {
				workspaceID = workspaceForDraft(extracted.LocalConfig.Teams, channelID, draft)
			}
			if workspaceID == "" {
				if resolved, ok := resolveChannelWorkspace(channelID, "", a.candidates); ok {
					workspaceID = resolved
				} else if filter.workspaceID == "" {
					workspaceID = firstWorkspaceID(extracted.LocalConfig.Teams)
				}
			}
		}
		owner, ok := a.resolve(channelID, workspaceID)
		if !ok {
			continue
		}
		if opts.DMPolicy == admission.Exclude {
			for _, destination := range draft.Destinations[1:] {
				other, admitted := a.resolve(destination.ChannelID, draft.WorkspaceID)
				if !admitted {
					ok = false
					break
				}
				if other != owner {
					a.summary.omit("ambiguous_workspace")
					ok = false
					break
				}
			}
		}
		if !ok {
			continue
		}
		p.drafts = append(p.drafts, ownedRecord[Draft]{draft, owner})
		p.data.Drafts = append(p.data.Drafts, draft)
	}
	p.data.RootState.Summary.AppTeamsKeys = filter.workspaceIDs(extracted.RootState.Summary.AppTeamsKeys)
	if opts.DMPolicy == admission.Exclude {
		p.summary.UnattributedMetadata = extracted.RootState.Summary.DownloadItemCount + countExpandables(extracted.Expandables)
		p.data.RootState.Downloads = nil
		p.data.RootState.Summary.DownloadItemCount = 0
		p.data.RootState.Summary.DownloadTeamCount = 0
		p.data.Expandables = nil
	}
	if !extracted.IndexedDB.NodeAvailable && extracted.IndexedDB.CandidateCount > 0 {
		p.summary.DecoderUnavailable = extracted.IndexedDB.CandidateCount
	}
	p.summary.DecodeFailures = extracted.IndexedDB.DecodeFailureCount
	return p, nil
}
