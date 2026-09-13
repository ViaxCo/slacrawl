package slackdesktop

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func LoadRootState(path string) (RootStateData, error) {
	data, err := os.ReadFile(path) //nolint:gosec // Reads the explicit Slack desktop state file selected by discovery.
	if err != nil {
		return RootStateData{}, err
	}

	var state rootState
	if err := json.Unmarshal(data, &state); err != nil {
		return RootStateData{}, err
	}

	keys := make([]string, 0, len(state.AppTeams))
	for key := range state.AppTeams {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	downloadItemCount := 0
	for _, teamDownloads := range state.Downloads {
		downloadItemCount += len(teamDownloads)
	}

	return RootStateData{
		Summary: RootStateSummary{
			AppTeamsKeys:      keys,
			WorkspaceCount:    len(state.Workspaces),
			TeamsCount:        len(state.Teams),
			DownloadTeamCount: len(state.Downloads),
			DownloadItemCount: downloadItemCount,
		},
		Downloads: state.Downloads,
	}, nil
}

type localStorageData struct {
	Summary     LocalStorageSummary
	LocalConfig LocalConfig
	Drafts      []Draft
	Activity    map[string]ActivitySession
	Recent      map[string][]string
	ReadMarkers []ReadMarker
	Statuses    []CustomStatusRecord
	Expandables []ExpandableRecord
}

func ParseLocalStorage(path string) (localStorageData, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return localStorageData{}, err
	}
	defer func() { _ = db.Close() }()

	var (
		configData LocalConfig
		drafts     []Draft
		activity   = map[string]ActivitySession{}
		recent     = map[string][]string{}
		markers    []ReadMarker
		statuses   []CustomStatusRecord
		expand     []ExpandableRecord
	)

	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := cleanKey(iter.Key())
		if !strings.HasPrefix(key, "_https://app.slack.com") {
			continue
		}
		value := jsonPayload(iter.Value())
		if len(value) == 0 {
			continue
		}

		switch {
		case strings.Contains(key, "localConfig_v2"):
			var payload struct {
				Teams map[string]DesktopTeam `json:"teams"`
			}
			if err := json.Unmarshal(value, &payload); err == nil {
				configData.Teams = payload.Teams
			}
		case strings.Contains(key, "persist-v1::") && strings.HasSuffix(key, "::drafts"):
			workspaceID, userID, ok := persistContext(key)
			if !ok {
				continue
			}
			var payload DraftsState
			if err := json.Unmarshal(value, &payload); err == nil {
				for id, draft := range payload.UnifiedDrafts {
					if draft.ClientDraftID == "" {
						draft.ClientDraftID = id
					}
					if draft.ID == "" {
						draft.ID = id
					}
					draft.WorkspaceID = workspaceID
					draft.UserID = userID
					drafts = append(drafts, draft)
				}
			}
		case strings.Contains(key, "activitySession_"):
			teamID := strings.TrimPrefix(key, "_https://app.slack.comactivitySession_")
			var payload ActivitySession
			if err := json.Unmarshal(value, &payload); err == nil {
				activity[teamID] = payload
			}
		case strings.Contains(key, "persist-v1::") && strings.HasSuffix(key, "::recentlyJoinedChannels"):
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(value, &payload); err == nil {
				parts := strings.Split(key, "::")
				if len(parts) >= 2 {
					teamID := parts[1]
					for channelID := range payload {
						recent[teamID] = append(recent[teamID], channelID)
					}
				}
			}
		case strings.Contains(key, "persist-v1::") && strings.HasSuffix(key, "::customStatus"):
			workspaceID, userID, ok := persistContext(key)
			if !ok {
				continue
			}
			var payload map[string]CustomStatus
			if err := json.Unmarshal(value, &payload); err == nil {
				record := CustomStatusRecord{WorkspaceID: workspaceID, UserID: userID}
				for id, status := range payload {
					if status.ID == "" {
						status.ID = id
					}
					if status.UserID == "" {
						status.UserID = userID
					}
					record.Statuses = append(record.Statuses, status)
				}
				sort.Slice(record.Statuses, func(i, j int) bool {
					return record.Statuses[i].DateCreated < record.Statuses[j].DateCreated
				})
				statuses = append(statuses, record)
			}
		case strings.Contains(key, "persist-v1::") && strings.HasSuffix(key, "::persistedApiCalls"):
			workspaceID, userID, ok := persistContext(key)
			if !ok {
				continue
			}
			var payload map[string]persistedAPICall
			if err := json.Unmarshal(value, &payload); err == nil {
				for persistKey, call := range payload {
					if call.Method != "conversations.mark" {
						continue
					}
					channelID, _ := call.Args["channel"].(string)
					ts, _ := call.Args["ts"].(string)
					if channelID == "" || ts == "" {
						continue
					}
					markers = append(markers, ReadMarker{
						WorkspaceID: workspaceID,
						UserID:      userID,
						ChannelID:   channelID,
						TS:          ts,
						Reason:      call.Reason,
						PersistKey:  fallback(call.PersistKey, persistKey),
					})
				}
			}
		case strings.Contains(key, "persist-v1::") && strings.HasSuffix(key, "::expandables"):
			workspaceID, userID, ok := persistContext(key)
			if !ok {
				continue
			}
			var payload map[string]bool
			if err := json.Unmarshal(value, &payload); err == nil {
				record := ExpandableRecord{WorkspaceID: workspaceID, UserID: userID}
				for expandableKey := range payload {
					record.Keys = append(record.Keys, expandableKey)
				}
				sort.Strings(record.Keys)
				expand = append(expand, record)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return localStorageData{}, err
	}

	for teamID := range recent {
		sort.Strings(recent[teamID])
	}

	recentCount := 0
	for _, ids := range recent {
		recentCount += len(ids)
	}
	customStatusCount := 0
	for _, record := range statuses {
		customStatusCount += len(record.Statuses)
	}
	expandableCount := 0
	for _, record := range expand {
		expandableCount += len(record.Keys)
	}

	return localStorageData{
		Summary: LocalStorageSummary{
			WorkspaceCount:     len(configData.Teams),
			DraftCount:         len(drafts),
			ActivityTeamCount:  len(activity),
			RecentChannelCount: recentCount,
			ReadMarkerCount:    len(markers),
			CustomStatusCount:  customStatusCount,
			ExpandableCount:    expandableCount,
		},
		LocalConfig: configData,
		Drafts:      drafts,
		Activity:    activity,
		Recent:      recent,
		ReadMarkers: markers,
		Statuses:    statuses,
		Expandables: expand,
	}, nil
}

type persistedAPICall struct {
	Method     string         `json:"method"`
	Args       map[string]any `json:"args"`
	Reason     string         `json:"reason"`
	PersistKey string         `json:"persistKey"`
}

func ScanIndexedDB(path string) (IndexedDBSummary, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true, Comparer: indexedDBComparer{}})
	if err != nil {
		return IndexedDBSummary{}, err
	}
	defer func() { _ = db.Close() }()

	stores := map[string]struct{}{}
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := cleanKey(iter.Key())
		if !strings.Contains(key, "#objectStore-") {
			continue
		}
		idx := strings.Index(key, "#objectStore-")
		stores[key[idx+1:]] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return IndexedDBSummary{}, err
	}

	names := make([]string, 0, len(stores))
	for name := range stores {
		names = append(names, name)
	}
	sort.Strings(names)
	return IndexedDBSummary{ObjectStores: names}, nil
}

func formatDecodeFailures(failures map[string]int) string {
	if len(failures) == 0 {
		return "unknown failure"
	}
	stages := make([]string, 0, len(failures))
	for stage := range failures {
		stages = append(stages, stage)
	}
	sort.Strings(stages)
	parts := make([]string, 0, len(stages))
	for _, stage := range stages {
		parts = append(parts, fmt.Sprintf("%s=%d", stage, failures[stage]))
	}
	return strings.Join(parts, ", ")
}

type indexedDBComparer struct{}

func (indexedDBComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (indexedDBComparer) Name() string            { return "idb_cmp1" }
func (indexedDBComparer) Separator(dst, a, b []byte) []byte {
	return comparer.DefaultComparer.Separator(dst, a, b)
}

func (indexedDBComparer) Successor(dst, b []byte) []byte {
	return comparer.DefaultComparer.Successor(dst, b)
}

func cleanKey(key []byte) string {
	return strings.Map(func(r rune) rune {
		if r < 32 {
			return -1
		}
		return r
	}, string(key))
}

func jsonPayload(value []byte) []byte {
	for i, b := range value {
		if b == '{' || b == '[' {
			return value[i:]
		}
	}
	return nil
}
