package slackapi

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
)

func TestConcurrentSyncReturnsCancellationAfterFinalChannelCommit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/conversations.history", r.URL.Path)
		_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","text":"committed","ts":"1710000000.000100"}],"response_metadata":{"next_cursor":""}}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test"}, server.URL+"/", server.Client())
	client.WithLogger(slog.New(cancelOnFinalProgress{Handler: testProgressLogger(&logs).Handler(), cancel: cancel}))
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()

	// Both channels have committed when the final progress callback cancels.
	// No failed request can supply a worker error that masks lost cancellation.
	err := client.syncChannels(ctx, st, "T123", cancellationTestChannels(), SyncOptions{Concurrency: 2}, time.Now().UTC(), false, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, logs.String(), "state=failed")
	require.NotContains(t, logs.String(), "state=finished")
	rows, err := st.Messages(context.Background(), "T123", "", "", 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestConcurrentSyncPreservesWorkerErrorBeforeSiblingCancellation(t *testing.T) {
	started := make(chan struct{}, 2)
	releaseFailure := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/conversations.history", r.URL.Path)
		started <- struct{}{}
		if mustFormValues(r).Get("channel") == "C111" {
			select {
			case <-releaseFailure:
				_, _ = w.Write([]byte(`{"ok":false,"error":"synthetic_failure"}`))
			case <-r.Context().Done():
			}
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var logs bytes.Buffer
	client := NewWithOptions(config.Tokens{Bot: "xoxb-test"}, server.URL+"/", server.Client()).WithLogger(testProgressLogger(&logs))
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	result := make(chan error, 1)
	go func() {
		result <- client.syncChannels(ctx, st, "T123", cancellationTestChannels(), SyncOptions{Concurrency: 2}, time.Now().UTC(), false, nil)
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("concurrent history requests did not start")
		}
	}
	close(releaseFailure)
	err := <-result
	require.ErrorContains(t, err, "channel C111 history: synthetic_failure")
	require.NotErrorIs(t, err, context.Canceled)
	require.Contains(t, logs.String(), "state=failed")
	require.NotContains(t, logs.String(), "state=finished")
}

func cancellationTestChannels() []slack.Channel {
	return []slack.Channel{
		{GroupConversation: slack.GroupConversation{Conversation: slack.Conversation{ID: "C111"}}},
		{GroupConversation: slack.GroupConversation{Conversation: slack.Conversation{ID: "C222"}}},
	}
}

type cancelOnFinalProgress struct {
	slog.Handler
	cancel context.CancelFunc
}

func (h cancelOnFinalProgress) Handle(ctx context.Context, record slog.Record) error {
	var state string
	var done, total int64
	record.Attrs(func(attr slog.Attr) bool {
		switch attr.Key {
		case "state":
			state = attr.Value.String()
		case "done":
			done = attr.Value.Int64()
		case "total":
			total = attr.Value.Int64()
		}
		return true
	})
	if state == "progress" && done == 2 && total == 2 {
		h.cancel()
	}
	return h.Handler.Handle(ctx, record)
}
