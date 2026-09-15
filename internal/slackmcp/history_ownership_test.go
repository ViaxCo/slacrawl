package slackmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPHistoryCallChecksOwnership(t *testing.T) {
	for _, provider := range []providerKind{providerCodex, providerReference} {
		for _, mode := range []string{"before", "data", "empty", "malformed", "request-error", "cancel-before", "cancel-in-flight", "check-error", "current-data", "current-empty", "current-malformed", "current-error", "unguarded"} {
			t.Run(string(provider)+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				current := mode != "before" && mode != "unguarded"
				calls := 0
				requestFailure, checkFailure := errors.New(admissionCanary), errors.New("synthetic ownership check")
				client := &Client{pageSize: 100, maxPages: 2, mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
					calls++
					require.Equal(t, "history", tool)
					expected := map[string]any{"channel_id": "C123", "cursor": "", "oldest": "100", "limit": 100, "response_format": "detailed"}
					if provider == providerReference {
						expected = map[string]any{"channel_id": "C123", "limit": 100}
					}
					require.Equal(t, expected, args)
					if !strings.HasPrefix(mode, "current-") {
						current = false
					}
					if mode == "cancel-in-flight" {
						cancel()
					}
					if mode == "request-error" || mode == "current-error" {
						return "", requestFailure
					}
					if mode == "malformed" || mode == "current-malformed" {
						return "{" + admissionCanary, nil
					}
					text := admissionCanary + " <@UONE>"
					if mode == "empty" || mode == "current-empty" {
						text = ""
					}
					return historyOwnershipPayload(t, provider, text, ""), nil
				})}
				check := func() (bool, error) {
					if mode == "check-error" && !current {
						return false, checkFailure
					}
					return current, nil
				}
				if mode == "unguarded" {
					check = nil
				}
				if mode == "cancel-before" {
					cancel()
				}
				result, err := client.channelMessages(ctx, toolset{provider: provider, readChannel: "history"}, "TLOCAL", "C123", "100", check)
				expectedCalls := 1
				if mode == "before" || mode == "cancel-before" {
					expectedCalls = 0
				}
				require.Equal(t, expectedCalls, calls)
				switch mode {
				case "cancel-before", "cancel-in-flight":
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, channelPage{}, result)
				case "check-error":
					require.ErrorIs(t, err, checkFailure)
					require.Equal(t, channelPage{}, result)
				case "current-error":
					require.ErrorIs(t, err, requestFailure)
				case "current-malformed":
					require.Error(t, err)
				case "current-data", "unguarded":
					require.NoError(t, err)
					require.False(t, result.revoked)
					require.Len(t, result.Messages, 1)
					require.Equal(t, admissionCanary+" <@UONE>", result.Messages[0].Text)
				case "current-empty":
					require.NoError(t, err)
					require.False(t, result.revoked)
					require.Empty(t, result.Messages)
				default:
					require.NoError(t, err)
					require.Equal(t, channelPage{revoked: true}, result, "revoked raw text and coverage never reach parsing/materialization")
				}
			})
		}
	}
}

func TestMCPHistoryStopsTextPagination(t *testing.T) {
	for _, mode := range []string{"before-next", "during-next", "current", "repeated", "max-pages"} {
		t.Run(mode, func(t *testing.T) {
			calls, checks := 0, 0
			current := true
			client := &Client{pageSize: 100, maxPages: 3, mcp: threadWorkSession(func(_ context.Context, _ string, args map[string]any) (string, error) {
				calls++
				cursor := ""
				if calls > 1 {
					cursor = "private-cursor-canary"
				}
				require.Equal(t, cursor, args["cursor"])
				next := "private-cursor-canary"
				if calls == 2 && mode != "repeated" {
					next = ""
				}
				if calls == 2 && mode == "during-next" {
					current = false
				}
				return historyOwnershipPayload(t, providerCodex, admissionCanary, next), nil
			})}
			if mode == "max-pages" {
				client.maxPages = 1
			}
			result, err := client.channelMessages(context.Background(), toolset{provider: providerCodex, readChannel: "history"}, "TLOCAL", "C123", "", func() (bool, error) {
				checks++
				if mode == "before-next" && checks == 3 {
					current = false
				}
				return current, nil
			})
			if mode == "before-next" || mode == "max-pages" {
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, 2, calls)
			}
			switch mode {
			case "before-next", "during-next":
				require.NoError(t, err)
				require.Equal(t, channelPage{revoked: true}, result, "discard earlier materialization too")
			case "current":
				require.NoError(t, err)
				require.Len(t, result.Messages, 2)
				require.False(t, result.revoked)
			case "repeated":
				require.EqualError(t, err, "MCP pagination repeated cursor")
				require.NotContains(t, err.Error(), "private-cursor-canary")
			case "max-pages":
				require.EqualError(t, err, "MCP pagination exceeded max_pages=1")
			}
		})
	}
}

func TestMCPHistoryRevokedNativeCoverage(t *testing.T) {
	for _, flag := range []string{"has_more", "is_limited"} {
		for _, current := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/current=%t", flag, current), func(t *testing.T) {
				owned := true
				calls := 0
				client := &Client{mcp: threadWorkSession(func(context.Context, string, map[string]any) (string, error) {
					calls++
					owned = current
					raw, err := json.Marshal(map[string]any{"ok": true, flag: true, "messages": []map[string]any{{"ts": "1710000001.000000", "text": admissionCanary}}})
					return string(raw), err
				})}
				result, err := client.channelMessages(context.Background(), toolset{provider: providerReference, readChannel: "history"}, "TLOCAL", "C123", "", func() (bool, error) { return owned, nil })
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				if !current {
					require.Equal(t, channelPage{revoked: true}, result)
				} else {
					require.Equal(t, messageCoverage{more: flag == "has_more", limited: flag == "is_limited"}, result.coverage)
					require.Len(t, result.Messages, 1)
				}
			})
		}
	}
}

func historyOwnershipPayload(t *testing.T, provider providerKind, text, cursor string) string {
	t.Helper()
	var payload map[string]any
	if provider == providerReference {
		messages := []map[string]any{}
		if text != "" {
			messages = append(messages, map[string]any{"ts": "1710000001.000000", "text": text})
		}
		payload = map[string]any{"ok": true, "messages": messages}
	} else {
		messages := "Channel: fixture (C123)"
		if text != "" {
			messages += "\n\n=== Message from Fixture (UONE) at 2024-03-09T16:00:00Z === \nMessage TS: 1710000001.000000\n" + text
		}
		pagination := ""
		if cursor != "" {
			pagination = "next `" + cursor + "`"
		}
		payload = map[string]any{"messages": messages, "pagination_info": pagination}
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return string(raw)
}
