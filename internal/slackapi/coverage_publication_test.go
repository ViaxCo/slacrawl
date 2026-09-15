package slackapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSyncCoveragePublicationKeepsNewerPending(t *testing.T) {
	for _, source := range []string{SourceBot, SourceUser} {
		for _, order := range []string{"begin-before-publication", "publication-before-begin"} {
			t.Run(source+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				olderStore, newerStore := historyAttemptStores(t)
				_, err := olderStore.DB().ExecContext(ctx, "insert into sync_state values ('doctor','threads','coverage','full','2000-01-01T00:00:00Z')")
				require.NoError(t, err)
				const markerQuery = "select value,updated_at from sync_state where source_name='doctor' and entity_type='threads' and entity_id='coverage'"
				marker, err := olderStore.QueryReadOnly(ctx, markerQuery)
				require.NoError(t, err)
				tokens := config.Tokens{User: "fixture-user"}
				if source == SourceBot {
					tokens.Bot = "fixture-bot"
				}
				failure := errors.New("synthetic history failure")
				corrected := false
				newerHistoryCalls := 0
				newer := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
					if r.URL.Path == "/conversations.history" {
						newerHistoryCalls++
						if !corrected {
							return nil, failure
						}
					}
					return primaryOwnerResponse(r.URL.Path), nil
				})
				newer.now = func() time.Time { return time.Unix(1710000300, 0).UTC() }
				scope := store.APIHistoryScope{SourceName: source, WorkspaceID: "T123", ChannelID: "C123"}
				var pending store.APIHistoryState
				runNewer := func() {
					require.ErrorIs(t, newer.Sync(ctx, newerStore, SyncOptions{WorkspaceID: "T123"}), failure)
					var err error
					pending, err = newerStore.APIHistory(ctx, scope)
					require.NoError(t, err)
					require.NotNil(t, pending.Pending)
					require.NotEmpty(t, pending.Generation)
					require.Equal(t, "1710000300.000000", pending.PendingLatest)
				}
				usersCalls := 0
				older := primaryOwnerClient(t, tokens, func(r *http.Request, _ url.Values) (any, error) {
					if r.URL.Path == "/users.list" {
						usersCalls++
						// The older Sync has completed its history, but has not
						// entered coverage publication when this request is held.
						if order == "begin-before-publication" {
							runNewer()
						}
					}
					return primaryOwnerResponse(r.URL.Path), nil
				})
				older.now = func() time.Time { return time.Unix(1710000200, 0).UTC() }
				require.NoError(t, older.Sync(ctx, olderStore, SyncOptions{WorkspaceID: "T123"}))
				require.Equal(t, 1, usersCalls)
				published, err := olderStore.QueryReadOnly(ctx, markerQuery)
				require.NoError(t, err)
				if order == "publication-before-begin" {
					require.NotEqual(t, marker, published)
					marker = published
					runNewer()
				}
				published, err = olderStore.QueryReadOnly(ctx, markerQuery)
				require.NoError(t, err)
				require.Equal(t, marker, published, "blocked publication and later Begin both preserve the historical marker")
				after, err := olderStore.APIHistory(ctx, scope)
				require.NoError(t, err)
				require.Equal(t, pending, after)
				status, err := olderStore.Status(ctx)
				require.NoError(t, err)
				require.Equal(t, "partial", status.ThreadState)
				require.Equal(t, 1, newerHistoryCalls)
				corrected = true
				require.NoError(t, newer.Sync(ctx, newerStore, SyncOptions{WorkspaceID: "T123"}))
				require.Equal(t, 2, newerHistoryCalls)
				status, err = olderStore.Status(ctx)
				require.NoError(t, err)
				require.Equal(t, "full", status.ThreadState)
				after, err = olderStore.APIHistory(ctx, scope)
				require.NoError(t, err)
				require.Nil(t, after.Pending)
				require.Empty(t, after.Generation)
				require.Equal(t, "1710000300.000000", after.Latest)
			})
		}
	}
}
