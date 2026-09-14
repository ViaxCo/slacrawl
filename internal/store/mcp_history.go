package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

const MCPHistoryEntityType = "history_work_v1"

// Since is normalized by the MCP caller before it becomes part of the scope.
// Adapter isolation prevents one connector's completed window from covering another.
type MCPHistoryScope struct {
	WorkspaceID string
	ChannelID   string
	Adapter     string
	Since       string
}

type MCPHistoryState struct {
	Complete bool    `json:"complete"`
	Latest   string  `json:"latest"`
	Pending  *string `json:"pending"`
	Revision string  `json:"revision"`
}

type MCPHistoryOptions struct {
	Full       bool
	LatestOnly bool
}

type MCPHistoryWork struct {
	MCPHistoryScope
	Revision         string
	Oldest           string
	EnforceRetention bool
}

func (scope MCPHistoryScope) key() (string, error) {
	if strings.TrimSpace(scope.WorkspaceID) == "" || strings.TrimSpace(scope.ChannelID) == "" || (scope.Adapter != "codex" && scope.Adapter != "reference") {
		return "", errors.New("MCP history scope requires workspace, channel and discovered adapter")
	}
	key, err := json.Marshal([4]string{scope.WorkspaceID, scope.ChannelID, scope.Adapter, scope.Since})
	return string(key), err
}

// BeginMCPHistory reads coverage, chooses bounds and replaces the attempt revision
// under one write lock. Pending is a logical bound: an ordinary retry always
// reapplies today's retention floor, even when a prior Full began unbounded.
func (s *Store) BeginMCPHistory(ctx context.Context, scope MCPHistoryScope, opts MCPHistoryOptions) (MCPHistoryWork, bool, error) {
	key, err := scope.key()
	if err != nil {
		return MCPHistoryWork{}, false, err
	}
	q, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return MCPHistoryWork{}, false, err
	}
	defer rollback()
	queries := storedb.New(q)
	if err := rejectWorkspaceCollision(ctx, scope.WorkspaceID, "channel", scope.ChannelID, queries.GetChannelWorkspace); err != nil {
		return MCPHistoryWork{}, false, err
	}
	if opts.LatestOnly && !opts.Full && scope.Since == "" {
		var eligible bool
		err := q.QueryRowContext(ctx, `select exists (select 1 from messages m join channels c on c.id = m.channel_id and c.workspace_id = m.workspace_id
   where m.workspace_id = ? and m.channel_id = ? and m.ts <> '' and m.ts not like 'draft:%')
   or exists (select 1 from sync_state where source_name = ? and entity_type = ? and entity_id = ?)`,
			scope.WorkspaceID, scope.ChannelID, retentionFloorSource, retentionSeedEntityType, scope.WorkspaceID+"|"+scope.ChannelID).Scan(&eligible)
		if err != nil {
			return MCPHistoryWork{}, false, err
		}
		if !eligible {
			return MCPHistoryWork{}, false, nil
		}
	}
	state, err := loadMCPHistory(ctx, queries, key)
	if err != nil {
		return MCPHistoryWork{}, false, err
	}
	oldest := ""
	switch {
	case scope.Since != "":
		oldest = scope.Since
	case opts.Full:
	case state.Pending != nil:
		oldest = *state.Pending
	case state.Latest != "":
		latest, err := mcpHistoryTimestamp(state.Latest)
		if err != nil {
			return MCPHistoryWork{}, false, err
		}
		oldest = strconv.FormatFloat(math.Max(latest-time.Hour.Seconds(), 0), 'f', 6, 64)
	}
	state.Pending = new(oldest)
	state.Revision = rand.Text()
	floor, err := retentionFloor(ctx, q, scope.WorkspaceID, scope.ChannelID)
	if err != nil {
		return MCPHistoryWork{}, false, err
	}
	restore := opts.Full || scope.Since != ""
	enforce := ShouldEnforceRetention(oldest, floor, restore)
	if !restore {
		oldest = (ChannelSyncCursor{RetentionFloor: floor}).ApplyRetentionFloor(oldest)
		if oldest != "" && oldest == floor {
			oldest = previousMicrosecondTimestamp(oldest)
		}
	}
	if err := saveMCPHistory(ctx, queries, key, state); err != nil {
		return MCPHistoryWork{}, false, err
	}
	if err := commit(); err != nil {
		return MCPHistoryWork{}, false, err
	}
	return MCPHistoryWork{MCPHistoryScope: scope, Revision: state.Revision, Oldest: oldest, EnforceRetention: enforce}, true, nil
}

// CompleteMCPHistory records only this revision's admitted history. Empty scans
// still establish completion; neither replies nor higher-priority stored rows
// can substitute for the latest timestamp actually returned by history.
func (s *Store) CompleteMCPHistory(ctx context.Context, work MCPHistoryWork, latest string) (bool, error) {
	key, err := work.MCPHistoryScope.key()
	if err != nil {
		return false, err
	}
	if work.Revision == "" {
		return false, errors.New("MCP history completion requires an attempt revision")
	}
	if latest != "" {
		if _, err := mcpHistoryTimestamp(latest); err != nil {
			return false, err
		}
	}
	q, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return false, err
	}
	defer rollback()
	queries := storedb.New(q)
	if err := rejectWorkspaceCollision(ctx, work.WorkspaceID, "channel", work.ChannelID, queries.GetChannelWorkspace); err != nil {
		return false, err
	}
	state, err := loadMCPHistory(ctx, queries, key)
	if err != nil {
		return false, err
	}
	if state.Revision != work.Revision || state.Pending == nil {
		return false, nil
	}
	if latest != "" {
		state.Latest, err = MaxMCPHistoryTS(state.Latest, latest)
		if err != nil {
			return false, err
		}
	}
	state.Complete, state.Pending = true, nil
	if err := saveMCPHistory(ctx, queries, key, state); err != nil {
		return false, err
	}
	if err := commit(); err != nil {
		return false, err
	}
	return true, nil
}

func loadMCPHistory(ctx context.Context, q *storedb.Queries, key string) (MCPHistoryState, error) {
	value, err := q.GetSyncState(ctx, storedb.GetSyncStateParams{SourceName: "mcp", EntityType: MCPHistoryEntityType, EntityID: key})
	if errors.Is(err, sql.ErrNoRows) {
		return MCPHistoryState{}, nil
	}
	if err != nil {
		return MCPHistoryState{}, err
	}
	var state MCPHistoryState
	if json.Unmarshal([]byte(value), &state) != nil || state.Revision == "" || (!state.Complete && state.Pending == nil) {
		return MCPHistoryState{}, errors.New("invalid local MCP history checkpoint")
	}
	if state.Latest != "" {
		if _, err := mcpHistoryTimestamp(state.Latest); err != nil {
			return MCPHistoryState{}, errors.New("invalid local MCP history checkpoint")
		}
	}
	return state, nil
}

func saveMCPHistory(ctx context.Context, q *storedb.Queries, key string, state MCPHistoryState) error {
	value, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return q.SetSyncState(ctx, storedb.SetSyncStateParams{SourceName: "mcp", EntityType: MCPHistoryEntityType, EntityID: key, Value: string(value), UpdatedAt: formatDBTime(time.Now().UTC())})
}

// MaxMCPHistoryTS validates every history timestamp before local filtering and
// compares numerically, preserving the provider's raw message key in the record.
func MaxMCPHistoryTS(current, next string) (string, error) {
	nextValue, err := mcpHistoryTimestamp(next)
	if err != nil {
		return "", err
	}
	if current == "" {
		return next, nil
	}
	currentValue, err := mcpHistoryTimestamp(current)
	if err != nil {
		return "", err
	}
	if nextValue > currentValue {
		return next, nil
	}
	return current, nil
}

func mcpHistoryTimestamp(value string) (float64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, errors.New("MCP message timestamp is empty")
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, errors.New("MCP message timestamp is invalid")
	}
	return parsed, nil
}

func previousMicrosecondTimestamp(value string) string {
	parts := strings.SplitN(strings.TrimSpace(value), ".", 2)
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return value
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 6 {
		fraction = fraction[:6]
	}
	fraction += strings.Repeat("0", 6-len(fraction))
	microseconds, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return value
	}
	if microseconds == 0 {
		seconds--
		microseconds = 999999
	} else {
		microseconds--
	}
	return fmt.Sprintf("%d.%06d", seconds, microseconds)
}
