package slackdesktop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestReduxReingestRespectsPurge(t *testing.T) {
	for _, preparedBeforePurge := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared-before-purge=%v", preparedBeforePurge), func(t *testing.T) {
			ctx := context.Background()
			st := admissionStore(t)
			cutoff := time.Unix(1710000000, 0).UTC()
			state := retentionReduxState("T1", "C1", []ReduxMessage{
				{TS: "1709999999.999999", Text: "expired-root <@UEXPIRED>"},
				{TS: "1710000001.000000", ThreadTS: "1709999999.999999", Text: "expired-reply <@UEXPIRED>"},
				{TS: "1710000000.000000", Text: "cutoff-root <@UCURRENT>", ReplyCount: 1},
				{TS: "1710000002.000000", ThreadTS: "1710000000.000000", Text: "current-reply <@UCURRENT>"},
				{TS: "1710000003.000000", Text: "current-message <@UCURRENT>"},
			})
			require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, cutoff, ingestFilter{}))
			var prepared []preparedReduxState
			if preparedBeforePurge {
				summary := AdmissionSummary{}
				admitter := newDesktopAdmission([]ReduxDecodedState{state}, ingestFilter{}, admission.Default, &summary)
				var err error
				prepared, err = admitter.prepareRedux([]ReduxDecodedState{state})
				require.NoError(t, err)
				require.Equal(t, 5, summary.Messages)
			}
			report, err := st.PurgeMessages(ctx, store.PurgeOptions{Before: cutoff, WorkspaceID: "T1", Delete: true, RequireNoMedia: true})
			require.NoError(t, err)
			require.Equal(t, int64(2), report.Messages, "a newer reply expires with its older parent")
			before := reduxRetentionMessageSnapshot(t, st)
			if preparedBeforePurge {
				// Admission already ran, but persistence must consult the floor
				// established afterward, inside the actual batch transaction.
				require.NoError(t, ingestPreparedReduxStates(ctx, st, prepared, cutoff.Add(time.Hour), ingestFilter{}))
			} else {
				require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, cutoff.Add(time.Hour), ingestFilter{}))
			}
			requireNoDesktopCanary(t, st, "expired-root", "expired-reply", "UEXPIRED")
			rows, err := st.QueryReadOnly(ctx, "select ts, coalesce(thread_ts,'') as thread_ts from messages order by ts")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{
				{"ts": "1710000000.000000", "thread_ts": ""},
				{"ts": "1710000002.000000", "thread_ts": "1710000000.000000"},
				{"ts": "1710000003.000000", "thread_ts": ""},
			}, rows)
			after := reduxRetentionMessageSnapshot(t, st)
			for _, table := range []string{"message_events", "message_event_heads", "message_fts"} {
				require.Equal(t, before[table], after[table], table)
			}
			mentions, err := st.QueryReadOnly(ctx, "select ts,target_id,deleted_at from message_mentions order by ts")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{
				{"ts": "1710000000.000000", "target_id": "UCURRENT", "deleted_at": nil},
				{"ts": "1710000002.000000", "target_id": "UCURRENT", "deleted_at": nil},
				{"ts": "1710000003.000000", "target_id": "UCURRENT", "deleted_at": nil},
			}, mentions)
			floor, err := st.ChannelRetentionFloor(ctx, "T1", "C1")
			require.NoError(t, err)
			require.Equal(t, "1710000000.000000", floor)
		})
	}
}

func TestReduxRetentionAcrossBatchFlushes(t *testing.T) {
	ctx := context.Background()
	st := admissionStore(t)
	cutoff := time.Unix(1710000000, 0).UTC()
	var messages []ReduxMessage
	var expected []map[string]any
	for i := range 502 {
		ts, text := fmt.Sprintf("1709999999.%06d", i), "expired-batch <@UEXPIRED>"
		if i%2 == 1 {
			ts, text = fmt.Sprintf("1710000000.%06d", i), "current-batch <@UCURRENT>"
			expected = append(expected, map[string]any{"ts": ts})
		}
		messages = append(messages, ReduxMessage{TS: ts, Text: text})
	}
	state := retentionReduxState("T1", "C1", messages)
	require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, cutoff, ingestFilter{}))
	report, err := st.PurgeMessages(ctx, store.PurgeOptions{Before: cutoff, Delete: true, RequireNoMedia: true})
	require.NoError(t, err)
	require.Equal(t, int64(251), report.Messages)
	require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, cutoff.Add(time.Hour), ingestFilter{}))
	rows, err := st.QueryReadOnly(ctx, "select ts from messages order by ts")
	require.NoError(t, err)
	require.Equal(t, expected, rows, "the 500-row flush and final two rows each include expired and allowed data")
	requireNoDesktopCanary(t, st, "expired-batch", "UEXPIRED")
	for _, table := range []string{"message_events", "message_event_heads", "message_mentions", "message_fts"} {
		rows, err := st.QueryReadOnly(ctx, "select count(*) as n from "+table)
		require.NoError(t, err)
		require.Equal(t, int64(251), rows[0]["n"], table)
	}
}

func TestReduxRetentionUsesStrongestScope(t *testing.T) {
	for _, tc := range []struct {
		name, workspace, channel string
		floors                   [][3]string
		allowEarlier             bool
	}{
		{"no-floor", "T1", "C1", nil, true},
		{"global-strongest", "T1", "C1", [][3]string{{"workspace_floor", "*", "1710000100.000000"}, {"workspace_floor", "T1", "1710000050.000000"}, {"channel_floor", "T1|C1", "1710000025.000000"}}, false},
		{"workspace-strongest", "T1", "C1", [][3]string{{"workspace_floor", "*", "1710000025.000000"}, {"workspace_floor", "T1", "1710000100.000000"}, {"channel_floor", "T1|C1", "1710000050.000000"}}, false},
		{"channel-strongest", "T1", "C1", [][3]string{{"workspace_floor", "*", "1710000025.000000"}, {"workspace_floor", "T1", "1710000050.000000"}, {"channel_floor", "T1|C1", "1710000100.000000"}}, false},
		{"foreign-scopes", "T1", "C1", [][3]string{{"workspace_floor", "*", "1710000025.000000"}, {"workspace_floor", "T2", "1710000100.000000"}, {"channel_floor", "T2|C1", "1710000100.000000"}, {"channel_floor", "T1|C2", "1710000100.000000"}}, true},
		{"new-channel-workspace-floor", "T1", "CNEW", [][3]string{{"workspace_floor", "T1", "1710000100.000000"}}, false},
		{"new-workspace-global-floor", "TNEW", "CNEW", [][3]string{{"workspace_floor", "*", "1710000100.000000"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := admissionStore(t)
			for _, floor := range tc.floors {
				require.NoError(t, st.SetSyncState(ctx, "retention", floor[0], floor[1], floor[2]))
			}
			state := retentionReduxState(tc.workspace, tc.channel, []ReduxMessage{
				{TS: "1710000075.000000", Text: "earlier-candidate <@UEARLIER>"},
				{TS: "1710000100.000000", Text: "at-cutoff <@UCURRENT>"},
			})
			require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, time.Unix(1710000200, 0).UTC(), ingestFilter{}))
			expected := []map[string]any{{"ts": "1710000100.000000"}}
			if tc.allowEarlier {
				expected = append([]map[string]any{{"ts": "1710000075.000000"}}, expected...)
			}
			rows, err := st.QueryReadOnly(ctx, "select ts from messages order by ts")
			require.NoError(t, err)
			require.Equal(t, expected, rows)
			if !tc.allowEarlier {
				requireNoDesktopCanary(t, st, "earlier-candidate", "UEARLIER")
			}
		})
	}
}

func TestReduxRetentionAllowsExistingRowsByPriority(t *testing.T) {
	for _, tc := range []struct {
		source string
		rank   int
	}{
		{"api-user", 1},
		{"desktop-indexeddb", 3},
		{"mcp", 4},
	} {
		t.Run(tc.source, func(t *testing.T) {
			ctx := context.Background()
			st := admissionStore(t)
			cutoff := time.Unix(1710000000, 0).UTC()
			_, err := st.PurgeMessages(ctx, store.PurgeOptions{Before: cutoff, WorkspaceID: "T1", Delete: true})
			require.NoError(t, err)
			// An explicit restore can leave an exact row below the floor. Its
			// continued updates remain eligible, with normal source priority.
			original := store.Message{WorkspaceID: "T1", ChannelID: "C1", TS: "1709999999.000000", Text: "restored <@UORIGINAL>", NormalizedText: "restored", SourceName: tc.source, SourceRank: tc.rank, RawJSON: "{}", UpdatedAt: cutoff}
			require.NoError(t, st.UpsertMessage(ctx, original, reduxMentions(original.Text)))
			before := reduxRetentionMessageSnapshot(t, st)
			state := retentionReduxState("T1", "C1", []ReduxMessage{
				{TS: original.TS, Text: "desktop-update <@UCURRENT>"},
				{TS: "1709999998.000000", Text: "expired-neighbor <@UEXPIRED>"},
				{TS: "1710000001.000000", ThreadTS: original.TS, Text: "expired-new-reply <@UEXPIRED>"},
			})
			require.NoError(t, ingestReduxStates(ctx, st, []ReduxDecodedState{state}, cutoff.Add(time.Hour), ingestFilter{}))
			requireNoDesktopCanary(t, st, "expired-neighbor", "expired-new-reply", "UEXPIRED")
			rows, err := st.QueryReadOnly(ctx, "select ts,text,source_name,source_rank from messages")
			require.NoError(t, err)
			wantText, wantSource, wantRank := "desktop-update <@UCURRENT>", indexedDBSourceName, int64(3)
			if tc.rank < 3 {
				wantText, wantSource, wantRank = original.Text, tc.source, int64(tc.rank)
				require.Equal(t, before, reduxRetentionMessageSnapshot(t, st), "API row and derived data stay byte-equivalent")
			}
			require.Equal(t, []map[string]any{{"ts": original.TS, "text": wantText, "source_name": wantSource, "source_rank": wantRank}}, rows)
		})
	}
}

func retentionReduxState(workspace, channel string, messages []ReduxMessage) ReduxDecodedState {
	for i := range messages {
		messages[i].Channel, messages[i].Type = channel, "message"
	}
	return ReduxDecodedState{WorkspaceID: workspace, Channels: []ReduxChannel{{ID: channel, IsChannel: true, ContextTeamID: workspace}}, Messages: messages}
}

func reduxRetentionMessageSnapshot(t *testing.T, st *store.Store) map[string][]map[string]any {
	t.Helper()
	snapshot := map[string][]map[string]any{}
	for _, table := range desktopArchiveTables[3 : len(desktopArchiveTables)-1] {
		order := "rowid"
		if table == "message_event_heads" {
			order = "channel_id,ts,event_type,source_name"
		}
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table+" order by "+order)
		require.NoError(t, err)
		snapshot[table] = rows
	}
	return snapshot
}
