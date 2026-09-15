package slackdesktop

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func Ingest(ctx context.Context, st *store.Store, sourcePath string, opts IngestOptions) (Source, error) {
	source, err := Discover(sourcePath)
	if err != nil {
		return Source{}, err
	}
	if !source.Available {
		return source, nil
	}

	snapshot, err := SnapshotPath(sourcePath)
	if err != nil {
		return Source{}, err
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(snapshot.Root)) }()

	extracted, err := Extract(ctx, snapshot.Root)
	if err != nil {
		return Source{}, err
	}
	if extracted.IndexedDB.NodeAvailable && extracted.IndexedDB.CandidateCount > 0 && extracted.IndexedDB.DecodedBlobCount == 0 {
		return Source{}, fmt.Errorf(
			"desktop IndexedDB: failed to decode all %d candidate blob(s): %s",
			extracted.IndexedDB.CandidateCount,
			formatDecodeFailures(extracted.IndexedDB.DecodeFailures),
		)
	}
	if opts.ExcludeDrafts {
		// Drafts also produce channel hints, summaries and legacy cleanup writes.
		// Remove them before any derived state reaches the archive.
		extracted.Drafts = nil
	}
	prepared, err := prepareDesktop(extracted, opts)
	if err != nil {
		return Source{}, err
	}
	extracted = prepared.data
	source.Admission = &prepared.summary
	source.Summary = extracted.RootState.Summary
	source.Local = localSummary(extracted)
	source.IndexedDB = extracted.IndexedDB

	now := time.Now().UTC()
	filter := newIngestFilter(opts)
	filter.resolveKnownChannelIDs(desktopChannelIDs(extracted))
	source.Summary.AppTeamsKeys = filter.workspaceIDs(source.Summary.AppTeamsKeys)
	statusByWorkspaceUser := map[string][]CustomStatus{}
	for _, status := range extracted.Statuses {
		statusByWorkspaceUser[status.WorkspaceID+":"+status.UserID] = append(statusByWorkspaceUser[status.WorkspaceID+":"+status.UserID], status.Statuses...)
	}
	for teamID, team := range extracted.LocalConfig.Teams {
		if !filter.allowWorkspace(teamID) {
			continue
		}
		sanitized := team
		sanitized.Token = config.Redact(sanitized.Token)
		userPayload := map[string]any{
			"team":            sanitized,
			"custom_statuses": statusByWorkspaceUser[teamID+":"+team.UserID],
		}
		if err := st.UpsertWorkspace(ctx, store.Workspace{
			ID:        teamID,
			Name:      fallback(sanitized.Name, teamID),
			Domain:    sanitized.Domain,
			RawJSON:   store.MarshalRaw(sanitized),
			UpdatedAt: now,
		}); err != nil {
			return Source{}, err
		}
		if team.UserID != "" {
			if err := upsertDesktopUser(ctx, st, store.User{
				ID:          team.UserID,
				WorkspaceID: teamID,
				Name:        team.UserID,
				DisplayName: fallback(team.Name, team.UserID),
				Title:       userTitle(statusByWorkspaceUser[teamID+":"+team.UserID]),
				RawJSON:     store.MarshalRaw(userPayload),
				UpdatedAt:   now,
			}); err != nil {
				return Source{}, err
			}
		}
	}

	channelHints := map[string]store.Channel{}
	for _, record := range prepared.recent {
		channelID, workspaceID := record.value.channelID, record.value.persistWorkspaceID
		resolvedWorkspaceID := record.workspaceID
		mergeChannelHint(channelHints, store.Channel{
			ID:          channelID,
			WorkspaceID: resolvedWorkspaceID,
			Name:        channelID,
			Kind:        "desktop_recent",
			RawJSON:     store.MarshalRaw(map[string]any{"workspace_id": resolvedWorkspaceID, "persist_workspace_id": workspaceID, "channel_id": channelID, "source": "recentlyJoinedChannels"}),
			UpdatedAt:   now,
		})
	}
	for _, record := range prepared.markers {
		marker, workspaceID := record.value, record.workspaceID
		mergeChannelHint(channelHints, store.Channel{
			ID:          marker.ChannelID,
			WorkspaceID: workspaceID,
			Name:        marker.ChannelID,
			Kind:        "desktop_mark",
			RawJSON:     store.MarshalRaw(marker),
			UpdatedAt:   now,
		})
	}
	draftBatch := store.WriteBatch{Messages: make([]store.MessageWrite, 0, len(extracted.Drafts))}
	type legacyDraftDeletion struct {
		workspaceID string
		channelID   string
		ts          string
	}
	legacyDraftDeletions := make([]legacyDraftDeletion, 0)
	for _, record := range prepared.drafts {
		draft, workspaceID := record.value, record.workspaceID
		channelID := draft.Destinations[0].ChannelID

		mergeChannelHint(channelHints, store.Channel{
			ID:          channelID,
			WorkspaceID: workspaceID,
			Name:        inferredChannelName(channelID, draft),
			Kind:        "desktop_draft",
			RawJSON:     store.MarshalRaw(map[string]any{"workspace_id": workspaceID, "channel_id": channelID, "source": "draft"}),
			UpdatedAt:   now,
		})

		message := store.Message{
			ChannelID:      channelID,
			TS:             draftTS(draft),
			WorkspaceID:    workspaceID,
			UserID:         fallback(draft.UserID, extracted.LocalConfig.Teams[workspaceID].UserID),
			Subtype:        "desktop_draft",
			ClientMsgID:    draft.ClientDraftID,
			ThreadTS:       draft.Destinations[0].ThreadTS,
			Text:           draftText(draft),
			NormalizedText: strings.TrimSpace(draftText(draft)),
			SourceRank:     3,
			SourceName:     draftSourceName,
			RawJSON:        store.MarshalRaw(draft),
			UpdatedAt:      now,
		}
		if message.Text == "" {
			continue
		}
		draftBatch.Messages = append(draftBatch.Messages, store.MessageWrite{
			Message:                message,
			SkipWorkspaceCollision: true,
		})
		if legacyTS := legacyDraftTS(draft); legacyTS != message.TS {
			legacyDraftDeletions = append(legacyDraftDeletions, legacyDraftDeletion{
				workspaceID: workspaceID,
				channelID:   channelID,
				ts:          legacyTS,
			})
		}
	}
	if len(draftBatch.Messages) > 0 {
		if _, err := st.ApplyWriteBatch(ctx, draftBatch); err != nil {
			return Source{}, err
		}
	}
	for _, deletion := range legacyDraftDeletions {
		if _, err := st.DeleteMessageBySource(ctx, deletion.workspaceID, deletion.channelID, deletion.ts, draftSourceName); err != nil {
			return Source{}, err
		}
	}
	for _, channel := range channelHints {
		if err := st.UpsertChannel(ctx, channel); err != nil {
			return Source{}, err
		}
	}
	if err := ingestPreparedReduxStates(ctx, st, prepared.redux, now, filter); err != nil {
		return Source{}, err
	}

	if err := st.SetSyncState(ctx, sourceName, "root_state", "path", source.Path); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "root_state", "app_teams", strings.Join(source.Summary.AppTeamsKeys, ",")); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "draft_count", intString(source.Local.DraftCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "indexeddb", "object_stores", strings.Join(source.IndexedDB.ObjectStores, ",")); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "indexeddb", "decoded_state_count", intString(source.IndexedDB.DecodedStateCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "workspace_count", intString(source.Local.WorkspaceCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "activity_team_count", intString(source.Local.ActivityTeamCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "recent_channel_count", intString(source.Local.RecentChannelCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "read_marker_count", intString(source.Local.ReadMarkerCount)); err != nil {
		return Source{}, err
	}
	if err := st.SetSyncState(ctx, sourceName, "local_storage", "custom_status_count", intString(source.Local.CustomStatusCount)); err != nil {
		return Source{}, err
	}
	if opts.DMPolicy != admission.Exclude {
		if err := st.SetSyncState(ctx, sourceName, "local_storage", "expandable_count", intString(source.Local.ExpandableCount)); err != nil {
			return Source{}, err
		}
	}

	for teamID, downloads := range extracted.RootState.Downloads {
		if !filter.allowWorkspace(teamID) {
			continue
		}
		if err := st.SetSyncState(ctx, sourceName, "downloads", teamID, intString(len(downloads))); err != nil {
			return Source{}, err
		}
	}
	// Separate workspace keys from arbitrary legacy channel-only keys. Users
	// and calls still share the existing per-workspace/channel overwrite order.
	for _, record := range prepared.markers {
		marker := record.value
		key, _ := json.Marshal([2]string{record.workspaceID, marker.ChannelID})
		if err := st.SetSyncState(ctx, sourceName, "read_marker_v1", string(key), marker.TS); err != nil {
			return Source{}, err
		}
	}
	for _, expandable := range extracted.Expandables {
		if !filter.allowWorkspace(expandable.WorkspaceID) {
			continue
		}
		if err := st.SetSyncState(ctx, sourceName, "expandables", expandable.WorkspaceID+":"+expandable.UserID, intString(len(expandable.Keys))); err != nil {
			return Source{}, err
		}
	}
	for _, status := range extracted.Statuses {
		if !filter.allowWorkspace(status.WorkspaceID) {
			continue
		}
		if err := st.SetSyncState(ctx, sourceName, "custom_status", status.WorkspaceID+":"+status.UserID, intString(len(status.Statuses))); err != nil {
			return Source{}, err
		}
	}

	return source, nil
}

func upsertDesktopUser(ctx context.Context, st *store.Store, user store.User) error {
	err := st.UpsertUser(ctx, user)
	if err == nil {
		return nil
	}
	if store.IsWorkspaceCollision(err, "user") {
		return nil
	}
	return err
}

func localSummary(extracted ExtractedData) LocalStorageSummary {
	return LocalStorageSummary{
		WorkspaceCount:     len(extracted.LocalConfig.Teams),
		DraftCount:         len(extracted.Drafts),
		ActivityTeamCount:  len(extracted.Activity),
		RecentChannelCount: countRecentChannels(extracted.Recent),
		ReadMarkerCount:    len(extracted.ReadMarkers),
		CustomStatusCount:  countCustomStatuses(extracted.Statuses),
		ExpandableCount:    countExpandables(extracted.Expandables),
	}
}

func countRecentChannels(recent map[string][]string) int {
	total := 0
	for _, ids := range recent {
		total += len(ids)
	}
	return total
}

func countCustomStatuses(records []CustomStatusRecord) int {
	total := 0
	for _, record := range records {
		total += len(record.Statuses)
	}
	return total
}

func countExpandables(records []ExpandableRecord) int {
	total := 0
	for _, record := range records {
		total += len(record.Keys)
	}
	return total
}

func draftText(draft Draft) string {
	var builder strings.Builder
	for _, op := range draft.Ops {
		switch value := op.Insert.(type) {
		case string:
			builder.WriteString(value)
		default:
			continue
		}
	}
	return strings.TrimSpace(builder.String())
}

func draftTS(draft Draft) string {
	id := draftID(draft)
	if draft.LastUpdatedTS > 0 {
		return "draft:" + trimFloat(draft.LastUpdatedTS) + ":" + id
	}
	if draft.LastUpdated > 0 {
		return "draft:" + trimFloat(draft.LastUpdated) + ":" + id
	}
	return "draft:" + id
}

func legacyDraftTS(draft Draft) string {
	id := draftID(draft)
	if draft.LastUpdatedTS > 0 {
		return "draft:" + legacyTrimFloat(draft.LastUpdatedTS) + ":" + id
	}
	if draft.LastUpdated > 0 {
		return "draft:" + legacyTrimFloat(draft.LastUpdated) + ":" + id
	}
	return "draft:" + id
}

func draftID(draft Draft) string {
	id := fallback(draft.ClientDraftID, draft.ID)
	if draft.WorkspaceID == "" {
		return id
	}
	return draft.WorkspaceID + ":" + id
}

func trimFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func legacyTrimFloat(value float64) string {
	return strings.TrimRight(strings.TrimRight(trimFloat(value), "0"), ".")
}

func firstWorkspaceID(teams map[string]DesktopTeam) string {
	ids := make([]string, 0, len(teams))
	for id := range teams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func workspaceForDraft(teams map[string]DesktopTeam, channelID string, draft Draft) string {
	_ = channelID
	for workspaceID, team := range teams {
		if team.UserID != "" && hasDraftForWorkspace(workspaceID, draft) {
			return workspaceID
		}
	}
	return ""
}

func hasDraftForWorkspace(workspaceID string, draft Draft) bool {
	for _, destination := range draft.Destinations {
		if strings.HasPrefix(destination.ChannelID, "C") || strings.HasPrefix(destination.ChannelID, "D") || strings.HasPrefix(destination.ChannelID, "G") {
			return true
		}
		if strings.Contains(destination.ChannelID, workspaceID) {
			return true
		}
	}
	return false
}

func inferredChannelName(channelID string, draft Draft) string {
	_ = draft
	return channelID
}

func persistContext(key string) (workspaceID string, userID string, ok bool) {
	parts := strings.Split(key, "::")
	if len(parts) < 4 {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func userTitle(statuses []CustomStatus) string {
	active := activeStatus(statuses)
	if active == "" {
		return "desktop_local_user"
	}
	return "desktop_local_user | " + active
}

func activeStatus(statuses []CustomStatus) string {
	for _, status := range statuses {
		if !status.IsActive {
			continue
		}
		if status.Emoji != "" && status.Text != "" {
			return status.Emoji + " " + status.Text
		}
		if status.Text != "" {
			return status.Text
		}
		if status.Emoji != "" {
			return status.Emoji
		}
	}
	return ""
}

func mergeChannelHint(hints map[string]store.Channel, candidate store.Channel) {
	current, ok := hints[candidate.ID]
	if !ok {
		hints[candidate.ID] = candidate
		return
	}
	if channelHintPriority(candidate.Kind) < channelHintPriority(current.Kind) {
		hints[candidate.ID] = candidate
		return
	}
	if current.WorkspaceID == "" && candidate.WorkspaceID != "" {
		current.WorkspaceID = candidate.WorkspaceID
	}
	if current.Name == "" && candidate.Name != "" {
		current.Name = candidate.Name
	}
	if current.RawJSON == "" || current.RawJSON == "{}" {
		current.RawJSON = candidate.RawJSON
	}
	current.UpdatedAt = candidate.UpdatedAt
	hints[candidate.ID] = current
}

func channelHintPriority(kind string) int {
	switch kind {
	case "desktop_draft":
		return 1
	case "desktop_recent":
		return 2
	case "desktop_mark":
		return 3
	default:
		return 100
	}
}

func fallback(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func intString(value int) string {
	data, _ := json.Marshal(value)
	return string(data)
}
