package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/slack-go/slack/slacktest"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestTailDMPolicyFromCLIConfig(t *testing.T) {
	for _, setting := range []*bool{nil, new(true), new(false)} {
		for _, workspaces := range []int{1, 2} {
			name := "omitted"
			if setting != nil {
				name = fmt.Sprint(*setting)
			}
			t.Run(fmt.Sprintf("%s/workspaces=%d", name, workspaces), func(t *testing.T) {
				fixture := startTailCLI(t, setting, workspaces, false)
				peers := fixture.peers(t, workspaces)
				for _, peer := range peers {
					peer.send(t, "dm", cliTailMessage(peer.team, "D", "im", "dm-wire-canary"))
					peer.ack(t, "dm")
					peer.send(t, "public", cliTailMessage(peer.team, "C", "channel", "public-wire"))
					peer.ack(t, "public")
				}
				require.Zero(t, fixture.lookups.Load(), "native message types require no lookup")
				for _, peer := range peers {
					peer.send(t, "rename", map[string]any{"type": "channel_rename", "channel": map[string]any{"id": "C" + peer.team, "name": "renamed"}})
					peer.ack(t, "rename")
				}
				fixture.cancel()
				require.ErrorIs(t, fixture.result(t), context.Canceled)
				for _, peer := range peers {
					peer.closed(t)
				}
				st, err := store.OpenReadOnly(fixture.dbPath)
				require.NoError(t, err)
				defer st.Close()
				exclude := setting != nil && !*setting
				for _, peer := range peers {
					rows, err := st.Messages(context.Background(), peer.team, "D"+peer.team, "", 10)
					require.NoError(t, err)
					if exclude {
						require.Empty(t, rows)
					} else {
						require.Len(t, rows, 1, "omitted/true preserve delivered DMs without a user token")
					}
					rows, err = st.Messages(context.Background(), peer.team, "C"+peer.team, "", 10)
					require.NoError(t, err)
					require.Len(t, rows, 1)
				}
				channels, err := st.QueryReadOnly(context.Background(), "select name from channels")
				require.NoError(t, err)
				require.Len(t, channels, workspaces)
				for _, channel := range channels {
					require.Equal(t, "renamed", channel["name"])
				}
				if exclude {
					require.Equal(t, int32(workspaces), fixture.lookups.Load())
					assertTailCLICanaryAbsent(t, st, "dm-wire-canary")
				} else {
					require.Zero(t, fixture.lookups.Load())
				}
				assertTailCLICanaryAbsent(t, st, "lookup-wire-canary")
			})
		}
	}
}

func TestTailCLILookupFailureDoesNotAck(t *testing.T) {
	fixture := startTailCLI(t, new(false), 1, true)
	peer := fixture.peers(t, 1)[0]
	peer.send(t, "public", cliTailMessage(peer.team, "C", "channel", "public-wire"))
	peer.ack(t, "public")
	inner := cliTailMessage(peer.team, "C", "", "rejected-wire-canary")
	inner["subtype"] = "message_changed"
	inner["message"] = cliTailMessage(peer.team, "C", "", "rejected-wire-canary")
	peer.send(t, "rejected", inner)
	require.ErrorContains(t, fixture.result(t), "missing_scope")
	// Stop the fixture SDK and assert no rejected ACK was observed on the wire.
	// Cancellation can prevent queued ACK transmission; direct handler tests
	// separately assert that lookup failure never calls Ack.
	fixture.cancel()
	peer.closed(t)
	for envelope := range peer.acks {
		require.NotEqual(t, "rejected", envelope)
	}
	require.Equal(t, int32(1), fixture.lookups.Load())
	st, err := store.OpenReadOnly(fixture.dbPath)
	require.NoError(t, err)
	defer st.Close()
	assertTailCLICanaryAbsent(t, st, "rejected-wire-canary", "lookup-wire-canary")
}

type tailCLIFixture struct {
	cancel      context.CancelFunc
	results     chan error
	connections chan *tailCLIPeer
	lookups     atomic.Int32
	dbPath      string
}

type tailCLIPeer struct {
	team string
	conn *websocket.Conn
	acks chan string
	done chan struct{}
}

func startTailCLI(t *testing.T, setting *bool, workspaceCount int, lookupFailure bool) *tailCLIFixture {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Share.RepoPath = filepath.Join(dir, "share")
	cfg.Share.AutoUpdate = false
	cfg.Slack.Desktop.Enabled = false
	cfg.Slack.User.Enabled = false
	cfg.Sync.IncludeDMs = setting
	cfg.Sync.RepairEvery = "0s"
	st, err := store.Open(cfg.DBPath)
	require.NoError(t, err)
	for i := 1; i <= workspaceCount; i++ {
		team := fmt.Sprintf("T%d", i)
		botEnv, appEnv := "SLACRAWL_TAIL_BOT_"+team, "SLACRAWL_TAIL_APP_"+team
		t.Setenv(botEnv, "fixture-bot-"+team)
		t.Setenv(appEnv, "fixture-app-"+team)
		cfg.Workspaces = append(cfg.Workspaces, config.Workspace{ID: team, BotTokenEnv: botEnv, AppTokenEnv: appEnv})
		require.NoError(t, st.UpsertChannel(context.Background(), store.Channel{ID: "C" + team, WorkspaceID: team, Name: "original", Kind: "public_channel", UpdatedAt: time.Unix(1710000000, 0)}))
	}
	require.NoError(t, st.Close())
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, cfg.Save(path))
	ctx, cancel := context.WithCancel(context.Background())
	fixture := &tailCLIFixture{cancel: cancel, results: make(chan error, 1), connections: make(chan *tailCLIPeer, workspaceCount), dbPath: cfg.DBPath}
	server := slacktest.NewTestServer(func(server slacktest.Customize) {
		server.Handle("/auth.test", func(w http.ResponseWriter, r *http.Request) {
			team := strings.TrimPrefix(cliSlackToken(r), "fixture-bot-")
			writeTailCLIJSON(t, w, map[string]any{"ok": true, "team_id": team})
		})
		server.Handle("/apps.connections.open", func(w http.ResponseWriter, r *http.Request) {
			team := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer fixture-app-")
			writeTailCLIJSON(t, w, map[string]any{"ok": true, "url": r.Context().Value(slacktest.ServerWSContextKey).(string) + "?team=" + team})
		})
		server.Handle("/conversations.info", func(w http.ResponseWriter, r *http.Request) {
			fixture.lookups.Add(1)
			values := cliSlackForm(r)
			if lookupFailure {
				writeTailCLIJSON(t, w, map[string]any{"ok": false, "error": "missing_scope"})
				return
			}
			writeTailCLIJSON(t, w, map[string]any{"ok": true, "channel": map[string]any{"id": values.Get("channel"), "is_channel": true, "latest": map[string]any{"text": "lookup-wire-canary"}}})
		})
		server.Handle("/ws", func(w http.ResponseWriter, r *http.Request) {
			slacktest.Websocket(func(conn *websocket.Conn) {
				peer := &tailCLIPeer{team: r.URL.Query().Get("team"), conn: conn, acks: make(chan string, 8), done: make(chan struct{})}
				defer close(peer.done)
				defer close(peer.acks)
				_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
				if err := conn.WriteJSON(map[string]any{"type": "hello"}); err != nil {
					return
				}
				select {
				case fixture.connections <- peer:
				case <-ctx.Done():
					return
				}
				for {
					var response struct {
						EnvelopeID string `json:"envelope_id"`
					}
					if err := conn.ReadJSON(&response); err != nil {
						return
					}
					// Keep every frame already read, including during cancellation.
					// The buffer exceeds this fixture's total envelope count.
					peer.acks <- response.EnvelopeID
				}
			})(w, r)
		})
	})
	server.Start()
	t.Cleanup(server.Stop)
	t.Cleanup(cancel)
	app := &App{Stdout: io.Discard, Stderr: io.Discard, apiURL: server.GetAPIURL(), httpClient: &http.Client{Timeout: 5 * time.Second}}
	go func() { fixture.results <- app.Run(ctx, []string{"--config", path, "tail"}) }()
	return fixture
}

func (f *tailCLIFixture) peers(t *testing.T, count int) []*tailCLIPeer {
	t.Helper()
	var peers []*tailCLIPeer
	for range count {
		select {
		case peer := <-f.connections:
			peers = append(peers, peer)
		case err := <-f.results:
			t.Fatalf("tail stopped before connecting: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("tail did not connect")
		}
	}
	return peers
}

func (f *tailCLIFixture) result(t *testing.T) error {
	t.Helper()
	select {
	case err := <-f.results:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("tail did not return")
		return nil
	}
}

func (p *tailCLIPeer) send(t *testing.T, envelope string, event map[string]any) {
	t.Helper()
	require.NoError(t, p.conn.WriteJSON(map[string]any{"type": "events_api", "envelope_id": envelope, "payload": map[string]any{"type": "event_callback", "team_id": p.team, "event": event}}))
}

func (p *tailCLIPeer) ack(t *testing.T, envelope string) {
	t.Helper()
	select {
	case got := <-p.acks:
		require.Equal(t, envelope, got)
	case <-time.After(5 * time.Second):
		t.Fatal("wire ACK not received")
	}
}

func (p *tailCLIPeer) closed(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture WebSocket did not close")
	}
}

func cliTailMessage(team, prefix, kind, text string) map[string]any {
	return map[string]any{"type": "message", "channel": prefix + team, "channel_type": kind, "ts": "1710000000.000000", "user": "UFIXTURE", "text": text + " <@UFIXTURE>", "blocks": []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": text}}}, "files": []any{map[string]any{"id": "F" + prefix + team, "title": text}}}
}

func writeTailCLIJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

func assertTailCLICanaryAbsent(t *testing.T, st *store.Store, canaries ...string) {
	t.Helper()
	for _, table := range []string{"workspaces", "channels", "users", "messages", "message_events", "message_event_heads", "message_files", "message_mentions", "message_fts", "sync_state", "embedding_jobs"} {
		rows, err := st.QueryReadOnly(context.Background(), "select * from "+table)
		require.NoError(t, err)
		raw, err := json.Marshal(rows)
		require.NoError(t, err)
		for _, canary := range canaries {
			require.NotContains(t, string(raw), canary, table)
		}
	}
}
