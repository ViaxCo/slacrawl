package slackdesktop

import (
	"encoding/json"
	"os"
	"strconv"
)

const (
	localStorageDir = "Local Storage/leveldb"
	indexedDBDir    = "IndexedDB/https_app.slack.com_0.indexeddb.leveldb"
	rootStateFile   = "storage/root-state.json"
	sourceName      = "desktop"
	draftSourceName = "desktop-draft"
)

var makeSnapshotTempDir = os.MkdirTemp

type Source struct {
	Path      string              `json:"path"`
	Available bool                `json:"available"`
	Summary   RootStateSummary    `json:"summary"`
	Local     LocalStorageSummary `json:"local_storage"`
	IndexedDB IndexedDBSummary    `json:"indexeddb"`
	Snapshot  string              `json:"snapshot_path,omitempty"`
}

type IngestOptions struct {
	WorkspaceID     string
	Channels        []string
	ExcludeChannels []string
}

type ingestFilter struct {
	workspaceID      string
	channels         map[string]struct{}
	excludeChannels  map[string]struct{}
	excludeSelectors []channelSelector
	hasNameExclude   bool
}

type channelSelector struct {
	raw        string
	normalized string
	explicitID bool
}

type RootStateSummary struct {
	AppTeamsKeys      []string `json:"app_teams_keys"`
	WorkspaceCount    int      `json:"workspace_count"`
	TeamsCount        int      `json:"teams_count"`
	DownloadTeamCount int      `json:"download_team_count"`
	DownloadItemCount int      `json:"download_item_count"`
}

type LocalStorageSummary struct {
	WorkspaceCount     int `json:"workspace_count"`
	DraftCount         int `json:"draft_count"`
	ActivityTeamCount  int `json:"activity_team_count"`
	RecentChannelCount int `json:"recent_channel_count"`
	ReadMarkerCount    int `json:"read_marker_count"`
	CustomStatusCount  int `json:"custom_status_count"`
	ExpandableCount    int `json:"expandable_count"`
}

type IndexedDBSummary struct {
	ObjectStores       []string       `json:"object_stores"`
	NodeAvailable      bool           `json:"node_available"`
	BlobFileCount      int            `json:"blob_file_count"`
	CandidateCount     int            `json:"candidate_count"`
	DecodedBlobCount   int            `json:"decoded_blob_count"`
	DecodedStateCount  int            `json:"decoded_state_count"`
	DecodeFailureCount int            `json:"decode_failure_count"`
	DecodeFailures     map[string]int `json:"decode_failures,omitempty"`
	V8Versions         map[string]int `json:"v8_versions,omitempty"`
}

func (summary *IndexedDBSummary) recordDecodeFailure(stage string) {
	if summary.DecodeFailures == nil {
		summary.DecodeFailures = map[string]int{}
	}
	summary.DecodeFailures[stage]++
	summary.DecodeFailureCount++
}

func (summary *IndexedDBSummary) recordV8Version(version byte) {
	if summary.V8Versions == nil {
		summary.V8Versions = map[string]int{}
	}
	summary.V8Versions[strconv.Itoa(int(version))]++
}

type Snapshot struct {
	Root string
}

type ExtractedData struct {
	RootState   RootStateData
	LocalConfig LocalConfig
	Drafts      []Draft
	Activity    map[string]ActivitySession
	Recent      map[string][]string
	ReadMarkers []ReadMarker
	Statuses    []CustomStatusRecord
	Expandables []ExpandableRecord
	ReduxStates []ReduxDecodedState
	IndexedDB   IndexedDBSummary
}

type RootStateData struct {
	Summary   RootStateSummary
	Downloads map[string]map[string]DownloadRecord
}

type DownloadRecord struct {
	ID         string `json:"id"`
	TeamID     string `json:"teamId"`
	UserID     string `json:"userId"`
	URL        string `json:"url"`
	AppVersion string `json:"appVersion"`
	State      string `json:"downloadState"`
	Path       string `json:"downloadPath"`
}

type rootState struct {
	AppTeams   map[string]json.RawMessage           `json:"appTeams"`
	Downloads  map[string]map[string]DownloadRecord `json:"downloads"`
	Workspaces map[string]json.RawMessage           `json:"workspaces"`
	Teams      map[string]json.RawMessage           `json:"teams"`
}

type LocalConfig struct {
	Teams map[string]DesktopTeam `json:"teams"`
}

type DesktopTeam struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	URL        string      `json:"url"`
	Domain     string      `json:"domain"`
	Token      string      `json:"token,omitempty"`
	UserID     string      `json:"user_id"`
	UserLocale string      `json:"user_locale"`
	Icon       interface{} `json:"icon,omitempty"`
}

type DraftsState struct {
	UnifiedDrafts map[string]Draft `json:"unifiedDrafts"`
}

type Draft struct {
	WorkspaceID    string             `json:"workspace_id,omitempty"`
	UserID         string             `json:"user_id,omitempty"`
	ID             string             `json:"id"`
	ClientDraftID  string             `json:"client_draft_id"`
	IsFromComposer bool               `json:"is_from_composer"`
	DateCreated    float64            `json:"date_created"`
	LastUpdated    float64            `json:"last_updated"`
	LastUpdatedTS  float64            `json:"last_updated_ts"`
	Destinations   []DraftDestination `json:"destinations"`
	Ops            []DraftOp          `json:"ops"`
	FileIDs        []string           `json:"file_ids"`
}

type DraftDestination struct {
	ChannelID string `json:"channel_id"`
	ThreadTS  string `json:"thread_ts"`
	Broadcast bool   `json:"broadcast"`
}

type DraftOp struct {
	Insert     interface{}            `json:"insert"`
	Attributes map[string]interface{} `json:"attributes"`
}

type ActivitySession map[string]ActivityRecord

type ActivityRecord struct {
	ID           string `json:"id"`
	StartTime    int64  `json:"startTime"`
	LastActivity int64  `json:"lastActivity"`
	LastLogged   int64  `json:"lastLogged"`
}

type CustomStatus struct {
	ID          string `json:"id"`
	UserID      string `json:"user_id"`
	Text        string `json:"text"`
	Emoji       string `json:"emoji"`
	Duration    string `json:"duration"`
	IsActive    bool   `json:"is_active"`
	DateCreated int64  `json:"date_created"`
	DateExpire  int64  `json:"date_expire"`
}

type CustomStatusRecord struct {
	WorkspaceID string         `json:"workspace_id"`
	UserID      string         `json:"user_id"`
	Statuses    []CustomStatus `json:"statuses"`
}

type ReadMarker struct {
	WorkspaceID string `json:"workspace_id"`
	UserID      string `json:"user_id"`
	ChannelID   string `json:"channel_id"`
	TS          string `json:"ts"`
	Reason      string `json:"reason"`
	PersistKey  string `json:"persist_key"`
}

type ExpandableRecord struct {
	WorkspaceID string   `json:"workspace_id"`
	UserID      string   `json:"user_id"`
	Keys        []string `json:"keys"`
}
