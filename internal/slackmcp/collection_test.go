package slackmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPNativeCollectionPresence(t *testing.T) {
	for _, method := range []string{"catalog", "strict-catalog", "users", "history", "replies"} {
		for _, shape := range []string{"missing", "null", "empty", "object", "string", "number", "boolean"} {
			t.Run(method+"/"+shape, func(t *testing.T) {
				payload := map[string]any{"ok": true}
				field := nativeCollectionField(method)
				switch shape {
				case "null":
					payload[field] = nil
				case "empty":
					payload[field] = []any{}
				case "object":
					payload[field] = map[string]any{"text": admissionCanary}
				case "string":
					payload[field] = admissionCanary
				case "number":
					payload[field] = 42
				case "boolean":
					payload[field] = true
				}
				if shape != "empty" {
					payload["response_metadata"] = map[string]any{"next_cursor": coverageCursorCanary}
					payload["has_more"], payload["is_limited"] = true, true
				}
				raw, err := json.Marshal(payload)
				require.NoError(t, err)
				calls := 0
				client := &Client{pageSize: 123, searchLimit: 321, maxPages: 2, mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
					calls++
					wantTool, wantArgs := nativeCollectionRequest(method, "")
					require.Equal(t, wantTool, tool)
					require.Equal(t, wantArgs, args)
					return string(raw), nil
				})}
				result, err := callNativeCollection(client, method)
				require.Equal(t, 1, calls, "uncertified page cursors cannot drive another request")
				if shape == "empty" {
					require.NoError(t, err)
					requireNativeEmptyCollection(t, method, result)
				} else {
					want := nativeCollectionError(method)
					if shape != "missing" && shape != "null" {
						want = nativeCollectionDecodeError(method)
					}
					require.EqualError(t, err, want)
					require.Zero(t, result)
					require.NotContains(t, err.Error(), admissionCanary)
					require.NotContains(t, err.Error(), coverageCursorCanary)
				}
			})
		}
	}
}

func TestMCPNativeCollectionErrorPrecedence(t *testing.T) {
	for _, method := range []string{"catalog", "strict-catalog", "users", "history", "replies"} {
		for _, raw := range []string{"null", "{}", `{"ok":false}`, `{"ok":false,"error":"` + admissionCanary + `"}`, `{"ok":true,"error":"` + admissionCanary + `"}`, `{"ok":"true"}`, "{"} {
			t.Run(fmt.Sprintf("%s/%s", method, raw), func(t *testing.T) {
				calls := 0
				client := &Client{mcp: threadWorkSession(func(context.Context, string, map[string]any) (string, error) {
					calls++
					return raw, nil
				})}
				result, err := callNativeCollection(client, method)
				require.Equal(t, 1, calls)
				require.Zero(t, result)
				want := nativeCollectionError(method)
				switch raw {
				case "null", "{}", `{"ok":false}`:
					switch method {
					case "strict-catalog":
						want = "native MCP catalog did not report successful Slack response"
					case "history", "replies":
						want = "native MCP " + method + " did not report successful Slack response"
					}
				case `{"ok":"true"}`, "{":
					want = nativeCollectionDecodeError(method)
				default:
					want = nativeCollectionDecodePrefix(method) + "Slack API reported an error"
				}
				require.EqualError(t, err, want)
				require.NotContains(t, err.Error(), admissionCanary)
			})
		}
	}
}

func TestMCPNativeCatalogCollectionPagination(t *testing.T) {
	for _, method := range []string{"catalog", "strict-catalog", "users"} {
		for _, second := range []string{"empty-first", "missing", "null", "wrong-type"} {
			t.Run(method+"/"+second, func(t *testing.T) {
				calls := 0
				client := &Client{pageSize: 123, searchLimit: 321, maxPages: 3, mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
					calls++
					cursor := ""
					if calls > 1 {
						cursor = "next-page"
					}
					wantTool, wantArgs := nativeCollectionRequest(method, cursor)
					require.Equal(t, wantTool, tool)
					require.Equal(t, wantArgs, args)
					require.LessOrEqual(t, calls, 2)
					payload := map[string]any{"ok": true}
					field := nativeCollectionField(method)
					if calls == 1 {
						payload[field] = []map[string]any{{"id": "CONE", "name": "first"}}
						if second == "empty-first" {
							payload[field] = []any{}
						}
						payload["response_metadata"] = map[string]any{"next_cursor": "next-page"}
					} else {
						switch second {
						case "empty-first":
							payload[field] = []map[string]any{{"id": "CONE", "name": "first"}}
						case "null":
							payload[field] = nil
						case "wrong-type":
							payload[field] = admissionCanary
						}
						if second != "empty-first" {
							payload["response_metadata"] = map[string]any{"next_cursor": coverageCursorCanary}
						}
					}
					raw, err := json.Marshal(payload)
					return string(raw), err
				})}
				result, err := callNativeCollection(client, method)
				require.Equal(t, 2, calls)
				if second == "empty-first" {
					require.NoError(t, err, "an explicit empty page can continue to another page")
				} else {
					want := nativeCollectionError(method)
					if second == "wrong-type" {
						want = nativeCollectionDecodeError(method)
					}
					require.EqualError(t, err, want)
				}
				// collectPages intentionally returns the valid prefix with its
				// error. The caller must reject the errored catalog as before.
				if method == "users" {
					users := result.([]UserRecord)
					require.Len(t, users, 1)
					require.Equal(t, "CONE", users[0].ID)
				} else {
					channels := result.([]ChannelRecord)
					require.Len(t, channels, 1)
					require.Equal(t, "CONE", channels[0].ID)
				}
			})
		}
	}
}

func TestMCPNativeEmptyCollectionsKeepCoverageFlags(t *testing.T) {
	for _, method := range []string{"history", "replies"} {
		for _, flag := range []string{"has_more", "is_limited", "next_cursor"} {
			t.Run(method+"/"+flag, func(t *testing.T) {
				payload := map[string]any{"ok": true, "messages": []any{}, flag: true}
				if flag == "next_cursor" {
					delete(payload, flag)
					payload["response_metadata"] = map[string]any{"next_cursor": coverageCursorCanary}
				}
				raw, err := json.Marshal(payload)
				require.NoError(t, err)
				calls := 0
				client := &Client{mcp: threadWorkSession(func(context.Context, string, map[string]any) (string, error) {
					calls++
					return string(raw), nil
				})}
				result, err := callNativeCollection(client, method)
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				want := messageCoverage{more: flag != "is_limited", limited: flag == "is_limited"}
				if method == "history" {
					require.Equal(t, want, result.(channelPage).coverage)
					require.Empty(t, result.(channelPage).Messages)
				} else {
					require.Equal(t, want, result.(threadPage).coverage)
					require.Nil(t, result.(threadPage).Parent)
					require.Empty(t, result.(threadPage).Replies)
				}
			})
		}
	}
}

func TestMCPNativeUsersEmptyCollectionKeepsOKPolicy(t *testing.T) {
	for _, raw := range []string{`{"members":[]}`, `{"ok":false,"members":[]}`} {
		t.Run(raw, func(t *testing.T) {
			calls := 0
			client := &Client{searchLimit: 321, mcp: threadWorkSession(func(_ context.Context, tool string, args map[string]any) (string, error) {
				calls++
				require.Equal(t, "users", tool)
				require.Equal(t, map[string]any{"cursor": "", "limit": 200}, args)
				return raw, nil
			})}
			users, err := client.referenceUsers(context.Background(), toolset{searchUsers: "users"})
			require.NoError(t, err)
			require.Empty(t, users)
			require.Equal(t, 1, calls)
		})
	}
}

func callNativeCollection(client *Client, method string) (any, error) {
	ctx := context.Background()
	tools := toolset{provider: providerReference, searchChannels: "catalog", searchUsers: "users", readChannel: "history", readThread: "replies"}
	switch method {
	case "catalog", "strict-catalog":
		return client.referenceChannels(ctx, tools, method == "strict-catalog")
	case "users":
		return client.referenceUsers(ctx, tools)
	case "history":
		return client.channelMessages(ctx, tools, "TLOCAL", "C123", "", nil)
	default:
		return client.threadMessages(ctx, tools, "TLOCAL", "C123", "1710000001.000000", nil)
	}
}

func nativeCollectionRequest(method, cursor string) (string, map[string]any) {
	switch method {
	case "catalog", "strict-catalog":
		return "catalog", map[string]any{"cursor": cursor, "limit": 200}
	case "users":
		return "users", map[string]any{"cursor": cursor, "limit": 200}
	case "history":
		return "history", map[string]any{"channel_id": "C123", "limit": 123}
	default:
		return "replies", map[string]any{"channel_id": "C123", "thread_ts": "1710000001.000000"}
	}
}

func nativeCollectionField(method string) string {
	switch method {
	case "catalog", "strict-catalog":
		return "channels"
	case "users":
		return "members"
	default:
		return "messages"
	}
}

func nativeCollectionError(method string) string {
	if method == "strict-catalog" {
		method = "catalog"
	}
	return "native MCP " + method + " did not provide a " + nativeCollectionField(method) + " array; page remains uncertified"
}

func nativeCollectionDecodePrefix(method string) string {
	label := map[string]string{"catalog": "channels", "strict-catalog": "channels", "users": "users", "history": "channel history", "replies": "thread"}[method]
	return "decode reference Slack " + label + ": "
}

func nativeCollectionDecodeError(method string) string {
	return nativeCollectionDecodePrefix(method) + "invalid Slack API response"
}

func requireNativeEmptyCollection(t *testing.T, method string, result any) {
	t.Helper()
	switch method {
	case "history":
		require.Equal(t, channelPage{ChannelID: "C123", Messages: []MessageRecord{}}, result)
	case "replies":
		require.Equal(t, threadPage{}, result)
	default:
		require.Empty(t, result)
	}
}
