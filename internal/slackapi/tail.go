package slackapi

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/openclaw/slacrawl/internal/store"
)

func (c *Client) Tail(ctx context.Context, st *store.Store, workspaceID string, repairEvery time.Duration) error {
	if c.bot == nil {
		return errors.New("SLACK_BOT_TOKEN is required for tail")
	}
	if c.appToken == "" {
		return errors.New("SLACK_APP_TOKEN is required for tail")
	}

	auth, err := c.authTest(ctx, c.bot)
	if err != nil {
		return err
	}
	workspaceID, err = authenticatedWorkspaceID(auth, workspaceID)
	if err != nil {
		return err
	}

	socketClient := c.socketModeFn(c.bot)
	errCh := make(chan error, 1)
	go func() {
		errCh <- socketClient.Run(ctx)
	}()

	var ticker *time.Ticker
	if repairEvery > 0 {
		ticker = time.NewTicker(repairEvery)
		defer ticker.Stop()
	}

	for {
		select {
		case err := <-errCh:
			return err
		case event := <-socketClient.Events():
			if err := c.handleSocketModeEvent(ctx, st, workspaceID, socketClient, event); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-tickerChan(ticker):
			if err := c.repairWorkspace(ctx, st, workspaceID); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Repair is a periodic reconciliation sweep; a transient
				// network failure during one tick must not kill a long-running
				// tail daemon. The next tick retries, and store errors surface
				// through the event handler path regardless.
				c.warnLogger().Warn("tail repair sweep failed; will retry next interval",
					"workspace_id", workspaceID,
					"err", err,
				)
				continue
			}
			if err := st.SetSyncState(ctx, "tail", "repair", workspaceID, c.now().Format(time.RFC3339)); err != nil {
				return err
			}
		}
	}
}

func (c *Client) HandleEventsAPIEvent(ctx context.Context, st *store.Store, workspaceID string, event slackevents.EventsAPIEvent) error {
	allowed, err := c.admitTailEvent(ctx, st, workspaceID, event)
	if err != nil || !allowed {
		return err
	}
	now := c.now()
	switch ev := event.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		msg := messageFromEvent(ev)
		stored := toStoreMessage(workspaceID, msg, SourceBot, 2, rawMessagePayload(event), now)
		deleted := msg.SubType == "message_deleted" || msg.DeletedTimestamp != ""
		var err error
		if deleted {
			_, err = st.MarkMessageDeletedWithRetention(ctx, stored, toStoreMentions(msg))
		} else {
			_, err = st.UpsertMessageWithRetention(ctx, stored, toStoreMentions(msg))
		}
		if err != nil && c.skipMessageCollision(err, workspaceID, stored.ChannelID, stored.TS) {
			return nil
		}
		return err
	case *slackevents.ChannelRenameEvent:
		return st.RenameChannel(ctx, workspaceID, ev.Channel.ID, ev.Channel.Name)
	case *slackevents.ChannelArchiveEvent:
		return st.SetChannelArchived(ctx, workspaceID, ev.Channel, true)
	case *slackevents.ChannelUnarchiveEvent:
		return st.SetChannelArchived(ctx, workspaceID, ev.Channel, false)
	default:
		return nil
	}
}

func rawMessagePayload(event slackevents.EventsAPIEvent) any {
	callback, ok := event.Data.(*slackevents.EventsAPICallbackEvent)
	if !ok || callback.InnerEvent == nil {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(*callback.InnerEvent, &raw); err != nil {
		return nil
	}
	return rawMessageFieldsPayload(raw)
}

func rawMessageFieldsPayload(raw map[string]any) any {
	if nested, ok := raw["message"].(map[string]any); ok {
		if payload := rawMessageFieldsPayload(nested); payload != nil {
			return payload
		}
	}
	blocks, hasBlocks := raw["blocks"]
	attachments, hasAttachments := raw["attachments"]
	if !hasBlocks && !hasAttachments {
		return nil
	}
	return []any{blocks, attachments}
}

func (c *Client) handleSocketModeEvent(ctx context.Context, st *store.Store, workspaceID string, socketClient socketModeRunner, event socketmode.Event) error {
	switch event.Type {
	case socketmode.EventTypeConnected:
		return st.SetSyncState(ctx, "tail", "connection", workspaceID, c.now().Format(time.RFC3339))
	case socketmode.EventTypeEventsAPI:
		eventsAPIEvent, ok := event.Data.(slackevents.EventsAPIEvent)
		if !ok {
			return nil
		}
		if err := c.HandleEventsAPIEvent(ctx, st, workspaceID, eventsAPIEvent); err != nil {
			return err
		}
		if event.Request != nil {
			socketClient.Ack(*event.Request)
		}
		return nil
	default:
		return nil
	}
}

func (c *Client) repairWorkspace(ctx context.Context, st *store.Store, workspaceID string) error {
	if c.bot == nil {
		return errors.New("SLACK_BOT_TOKEN is required for repair")
	}
	userRepliesAvailable, err := c.userAuthAvailable(ctx, workspaceID)
	if err != nil {
		return err
	}
	channels, err := c.fetchChannels(ctx, workspaceID)
	if err != nil {
		return err
	}
	now := c.now()
	threadRepliesSkipped := newThreadSkipTracker()
	// Periodic repair shares ordinary coverage; a moving explicit-since scope
	// would strand its pending interval after a partially committed attempt.
	if err := c.syncChannels(ctx, st, workspaceID, channels, SyncOptions{
		enforceRetention: true,
	}, now, userRepliesAvailable, threadRepliesSkipped); err != nil {
		return err
	}
	if threadRepliesSkipped.Skipped() {
		return st.SetSyncState(ctx, "doctor", "threads", "coverage", "partial")
	}
	return nil
}

func messageFromEvent(event *slackevents.MessageEvent) slack.Message {
	msg := slack.Message{}
	if event.Message != nil {
		msg.Msg = *event.Message
	}
	if event.PreviousMessage != nil {
		if msg.Text == "" {
			msg.Text = event.PreviousMessage.Text
		}
		if msg.Timestamp == "" || event.SubType == "message_deleted" {
			msg.Timestamp = event.PreviousMessage.Timestamp
		}
		if msg.ThreadTimestamp == "" || event.SubType == "message_deleted" {
			msg.ThreadTimestamp = event.PreviousMessage.ThreadTimestamp
		}
		if msg.User == "" {
			msg.User = event.PreviousMessage.User
		}
	}
	if msg.Channel == "" {
		msg.Channel = event.Channel
	}
	if msg.User == "" {
		msg.User = event.User
	}
	if msg.Text == "" {
		msg.Text = event.Text
	}
	if msg.Timestamp == "" {
		msg.Timestamp = event.TimeStamp
	}
	if event.SubType == "message_deleted" && event.DeletedTimeStamp != "" {
		msg.Timestamp = event.DeletedTimeStamp
	}
	if msg.ThreadTimestamp == "" {
		msg.ThreadTimestamp = event.ThreadTimeStamp
	}
	if msg.SubType == "" {
		msg.SubType = event.SubType
	}
	if event.SubType == "message_deleted" && msg.DeletedTimestamp == "" {
		msg.DeletedTimestamp = event.DeletedTimeStamp
	}
	return msg
}

type socketModeRunner interface {
	Run(ctx context.Context) error
	Ack(req socketmode.Request, payload ...interface{})
	Events() <-chan socketmode.Event
}

type managedSocketMode struct {
	client *socketmode.Client
}

func (m managedSocketMode) Run(ctx context.Context) error { return m.client.RunContext(ctx) }
func (m managedSocketMode) Ack(req socketmode.Request, payload ...interface{}) {
	_ = m.client.Ack(req, payload...)
}
func (m managedSocketMode) Events() <-chan socketmode.Event { return m.client.Events }

func tickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
