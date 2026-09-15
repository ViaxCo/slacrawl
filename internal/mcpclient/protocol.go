package mcpclient

import (
	"context"
	"errors"
	"strings"
)

type rpcCall func(context.Context, string, any, any) error

func listTools(ctx context.Context, call rpcCall) ([]Tool, error) {
	var tools []Tool
	cursor := ""
	seen := map[string]bool{}
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page toolsPage
		if err := call(ctx, "tools/list", params, &page); err != nil {
			return nil, err
		}
		tools = append(tools, page.Tools...)
		if strings.TrimSpace(page.NextCursor) == "" {
			return tools, nil
		}
		if seen[page.NextCursor] {
			return nil, errors.New("MCP tools/list repeated cursor")
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

func callToolText(ctx context.Context, call rpcCall, name string, arguments map[string]any) (string, error) {
	var result toolCallResult
	if err := call(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": stripEmptyArguments(arguments),
	}, &result); err != nil {
		return "", err
	}
	if result.IsError {
		return "", errors.New("MCP tools/call reported an error")
	}
	parts := make([]string, 0, len(result.Content))
	for _, item := range result.Content {
		if item.Type != "" && item.Type != "text" {
			continue
		}
		if strings.TrimSpace(item.Text) != "" {
			parts = append(parts, item.Text)
		}
	}
	text := strings.Join(parts, "\n")
	if strings.TrimSpace(text) == "" {
		return "", errors.New("MCP tools/call returned no text content")
	}
	return text, nil
}
