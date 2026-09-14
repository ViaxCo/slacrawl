package store

import (
	"context"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

type APIThreadCoverageFacts struct {
	ThreadWork        bool
	IncompleteHistory bool
}

type APIThreadCoveragePublication struct {
	FullEligible       bool
	CleanupWorkspaceID string
}

func apiThreadCoverageFacts(ctx context.Context, dbtx storedb.DBTX) (APIThreadCoverageFacts, error) {
	incomplete, err := hasIncompleteAPIHistory(ctx, dbtx, "")
	if err != nil {
		return APIThreadCoverageFacts{}, err
	}
	facts := APIThreadCoverageFacts{IncompleteHistory: incomplete}
	q := storedb.New(dbtx)
	for _, kind := range []string{"thread_skip", ThreadPendingEntityType} {
		count, err := q.CountSyncStateByType(ctx, storedb.CountSyncStateByTypeParams{
			SourceName: "api-user", EntityType: kind,
		})
		if err != nil {
			return APIThreadCoverageFacts{}, err
		}
		facts.ThreadWork = facts.ThreadWork || count > 0
	}
	return facts, nil
}

// PublishAPIThreadCoverage serializes optional cleanup, retained facts and the
// marker write. A blocked eligible run leaves the historical marker untouched;
// Status projects current retained work without turning it into durable partial.
func (s *Store) PublishAPIThreadCoverage(ctx context.Context, publication APIThreadCoveragePublication) error {
	dbtx, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer rollback()
	if publication.FullEligible && publication.CleanupWorkspaceID != "" {
		if err := deleteAPIThreadSkipsIfNoPending(ctx, dbtx, publication.CleanupWorkspaceID); err != nil {
			return err
		}
	}
	// Validate the archive even for a genuine partial candidate. Any failure
	// after scoped cleanup rolls back that cleanup with the publication.
	facts, err := apiThreadCoverageFacts(ctx, dbtx)
	if err != nil {
		return err
	}
	if !publication.FullEligible || (!facts.ThreadWork && !facts.IncompleteHistory) {
		coverage := "partial"
		if publication.FullEligible {
			coverage = "full"
		}
		if err := storedb.New(dbtx).SetSyncState(ctx, storedb.SetSyncStateParams{
			SourceName: "doctor", EntityType: "threads", EntityID: "coverage",
			Value: coverage, UpdatedAt: formatDBTime(time.Now().UTC()),
		}); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return commit()
}
