package report

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestReportsRespectMicrosecondWindowBoundaries(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	ctx := context.Background()
	now := time.Date(2026, 4, 22, 12, 0, 0, 500000500, time.UTC)
	for _, channel := range []string{"C-quiet", "C-window", "C-boundary"} {
		require.NoError(t, st.UpsertChannel(ctx, store.Channel{ID: channel, WorkspaceID: "T1", Name: channel, UpdatedAt: now, RawJSON: "{}"}))
	}
	old := now.Add(-40 * 24 * time.Hour)
	addMsg(t, ctx, st, "C-quiet", slackTSBoundary(old), "T1", "U1", "", "last past activity")
	addMsg(t, ctx, st, "C-quiet", slackTSBoundary(now.Add(24*time.Hour)), "T1", "U1", "", "future activity")
	addMsg(t, ctx, st, "C-boundary", slackTSBoundary(now.Add(-30*24*time.Hour)), "T1", "U1", "", "just outside lookback")
	since := now.Add(-time.Hour)
	for _, ts := range []time.Time{since.Truncate(time.Microsecond), since.Truncate(time.Microsecond).Add(time.Microsecond), now.Truncate(time.Microsecond), now.Truncate(time.Microsecond).Add(time.Microsecond)} {
		addMsgWithMentions(t, ctx, st, "C-window", slackTSBoundary(ts), "T1", "U1", "", "window message", []store.Mention{{Type: "user", TargetID: "U2"}})
	}

	t.Run("quiet ignores future messages", func(t *testing.T) {
		quiet, err := BuildQuiet(ctx, st, QuietOptions{Now: now, WorkspaceID: "T1"})
		require.NoError(t, err)
		require.Len(t, quiet.Channels, 2)
		require.Equal(t, "C-quiet", quiet.Channels[0].ChannelID)
		require.Equal(t, old.Format(time.RFC3339), quiet.Channels[0].LastMessage)
		require.Equal(t, "C-boundary", quiet.Channels[1].ChannelID)
	})
	t.Run("digest lower and upper bounds", func(t *testing.T) {
		digest, err := BuildDigest(ctx, st, DigestOptions{Now: now, Since: time.Hour, Channel: "C-window"})
		require.NoError(t, err)
		require.Equal(t, 2, digest.Totals.Messages)
		require.Len(t, digest.Channels, 1)
		require.Equal(t, 2, digest.Channels[0].Messages)
		require.Equal(t, 2, digest.Channels[0].TopPosters[0].Count)
		require.Equal(t, 2, digest.Channels[0].TopMentions[0].Count)
	})
	t.Run("trends excludes future microseconds", func(t *testing.T) {
		trends, err := BuildTrends(ctx, st, TrendsOptions{Now: now, Weeks: 1, Channel: "C-window"})
		require.NoError(t, err)
		require.Len(t, trends.Rows, 1)
		require.Equal(t, 3, trends.Rows[0].Weekly[0].Messages)
	})
}

func TestDigestCountsThreadRootsByChannelAndMetadata(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	ctx := context.Background()
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	ts := slackTSBoundary(now.Add(-time.Hour))
	for _, channel := range []string{"C1", "C2", "C3"} {
		message := store.Message{WorkspaceID: "T1", ChannelID: channel, TS: ts, ThreadTS: ts, SourceName: "api-bot", SourceRank: 2, ReplyCount: 1, RawJSON: "{}", UpdatedAt: now}
		if channel == "C3" {
			message.ThreadTS = ""
		}
		require.NoError(t, st.UpsertMessage(ctx, message, nil))
	}
	digest, err := BuildDigest(ctx, st, DigestOptions{Now: now})
	require.NoError(t, err)
	require.Equal(t, 3, digest.Totals.Threads)
	require.Len(t, digest.Channels, 3)
	for _, channel := range digest.Channels {
		require.Equal(t, 1, channel.Threads, channel.ChannelID)
	}
}
