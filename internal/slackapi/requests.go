package slackapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/slack-go/slack"
)

func (c *Client) getConversations(ctx context.Context, params *slack.GetConversationsParameters) ([]slack.Channel, string, error) {
	type result struct {
		channels   []slack.Channel
		nextCursor string
	}
	res, err := retry(ctx, c.sleep, 3, func() (result, error) {
		channels, nextCursor, err := c.bot.GetConversationsContext(ctx, params)
		return result{channels: channels, nextCursor: nextCursor}, err
	})
	return res.channels, res.nextCursor, err
}

type rawConversationMessage struct {
	Message    slack.Message
	RawPayload any
}

type conversationHistoryPage struct {
	Messages   []rawConversationMessage
	NextCursor string
}

type conversationRepliesPage struct {
	Messages   []rawConversationMessage
	HasMore    bool
	NextCursor string
}

type rawConversationHistoryResponse struct {
	slack.SlackResponse
	HasMore          bool   `json:"has_more"`
	PinCount         int    `json:"pin_count"`
	Latest           string `json:"latest"`
	ResponseMetaData struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
	Messages []json.RawMessage `json:"messages"`
}

type rawConversationRepliesResponse struct {
	slack.SlackResponse
	HasMore          bool `json:"has_more"`
	ResponseMetaData struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
	Messages []json.RawMessage `json:"messages"`
}

func (c *Client) getConversationHistory(ctx context.Context, token string, params *slack.GetConversationHistoryParameters) (*conversationHistoryPage, error) {
	return retry(ctx, c.sleep, 3, func() (*conversationHistoryPage, error) {
		values := conversationHistoryValues(params)
		resp := rawConversationHistoryResponse{}
		if err := c.postSlackForm(ctx, token, "conversations.history", values, &resp); err != nil {
			return nil, err
		}
		if err := resp.Err(); err != nil {
			return nil, err
		}
		messages, err := rawConversationMessages(resp.Messages)
		if err != nil {
			return nil, err
		}
		return &conversationHistoryPage{Messages: messages, NextCursor: resp.ResponseMetaData.NextCursor}, nil
	})
}

func (c *Client) getConversationReplies(ctx context.Context, params *slack.GetConversationRepliesParameters) (*conversationRepliesPage, error) {
	return retry(ctx, c.sleep, 3, func() (*conversationRepliesPage, error) {
		values := conversationRepliesValues(params)
		resp := rawConversationRepliesResponse{}
		if err := c.postSlackForm(ctx, c.tokens.User, "conversations.replies", values, &resp); err != nil {
			return nil, err
		}
		if err := resp.Err(); err != nil {
			return nil, err
		}
		messages, err := rawConversationMessages(resp.Messages)
		if err != nil {
			return nil, err
		}
		return &conversationRepliesPage{
			Messages:   messages,
			HasMore:    resp.HasMore,
			NextCursor: resp.ResponseMetaData.NextCursor,
		}, nil
	})
}

func (c *Client) postSlackForm(ctx context.Context, token string, method string, values url.Values, target any) error {
	values.Set("token", token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL+method, strings.NewReader(values.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusTooManyRequests && resp.Header.Get("Retry-After") != "" {
		seconds, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64)
		if err != nil {
			return err
		}
		return &slack.RateLimitedError{RetryAfter: time.Duration(seconds) * time.Second}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack %s: %s", method, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("slack %s response: %w", method, err)
	}
	return nil
}

func conversationHistoryValues(params *slack.GetConversationHistoryParameters) url.Values {
	values := url.Values{"channel": {params.ChannelID}}
	if params.Cursor != "" {
		values.Set("cursor", params.Cursor)
	}
	if params.Inclusive {
		values.Set("inclusive", "1")
	} else {
		values.Set("inclusive", "0")
	}
	if params.Latest != "" {
		values.Set("latest", params.Latest)
	}
	if params.Limit != 0 {
		values.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Oldest != "" {
		values.Set("oldest", params.Oldest)
	}
	if params.IncludeAllMetadata {
		values.Set("include_all_metadata", "1")
	} else {
		values.Set("include_all_metadata", "0")
	}
	return values
}

func conversationRepliesValues(params *slack.GetConversationRepliesParameters) url.Values {
	values := url.Values{
		"channel": {params.ChannelID},
		"ts":      {params.Timestamp},
	}
	if params.Cursor != "" {
		values.Set("cursor", params.Cursor)
	}
	if params.Latest != "" {
		values.Set("latest", params.Latest)
	}
	if params.Limit != 0 {
		values.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Oldest != "" {
		values.Set("oldest", params.Oldest)
	}
	if params.Inclusive {
		values.Set("inclusive", "1")
	} else {
		values.Set("inclusive", "0")
	}
	if params.IncludeAllMetadata {
		values.Set("include_all_metadata", "1")
	} else {
		values.Set("include_all_metadata", "0")
	}
	return values
}

func rawConversationMessages(rawMessages []json.RawMessage) ([]rawConversationMessage, error) {
	messages := make([]rawConversationMessage, 0, len(rawMessages))
	for _, raw := range rawMessages {
		var msg slack.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			return nil, err
		}
		messages = append(messages, rawConversationMessage{
			Message:    msg,
			RawPayload: rawPayloadFromMessageJSON(raw),
		})
	}
	return messages, nil
}

func rawPayloadFromMessageJSON(rawMessage json.RawMessage) any {
	var raw map[string]any
	if err := json.Unmarshal(rawMessage, &raw); err != nil {
		return nil
	}
	return rawMessageFieldsPayload(raw)
}

func (c *Client) getUsers(ctx context.Context, client *slack.Client) ([]slack.User, error) {
	var (
		cursor string
		users  []slack.User
		seen   = map[string]bool{}
	)
	for {
		type result struct {
			users      []slack.User
			nextCursor string
		}
		page, err := retry(ctx, c.sleep, 3, func() (result, error) {
			pager := client.GetUsersPaginated(slack.GetUsersOptionLimit(200), slack.GetUsersOptionCursor(cursor))
			next, callErr := pager.Next(ctx)
			if callErr != nil {
				return result{}, callErr
			}
			return result{users: next.Users, nextCursor: next.Cursor}, nil
		})
		if err != nil {
			return nil, err
		}
		users = append(users, page.users...)
		if page.nextCursor == "" {
			return users, nil
		}
		if seen[page.nextCursor] {
			return nil, fmt.Errorf("users.list repeated cursor %q", page.nextCursor)
		}
		seen[page.nextCursor] = true
		cursor = page.nextCursor
	}
}
