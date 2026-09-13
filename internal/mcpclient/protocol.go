package mcpclient

import (
	"context"
	"fmt"
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
			return nil, fmt.Errorf("MCP tools/list repeated cursor %q", page.NextCursor)
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
	if result.IsError {
		if text == "" {
			return "", fmt.Errorf("MCP tool %q reported an error", name)
		}
		return "", fmt.Errorf("MCP tool %q reported an error: %s", name, text)
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("MCP tool %q returned no text content", name)
	}
	return text, nil
}
