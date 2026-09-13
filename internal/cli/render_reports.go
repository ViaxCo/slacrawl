package cli

import (
	"fmt"
	"strings"
)

func renderReportBlock(w *strings.Builder, value any) bool {
	report, ok := value.(map[string]any)
	if !ok {
		return false
	}
	writeTitle(w, "REPORT")
	if activity, ok := report["activity"].(map[string]any); ok {
		w.WriteString(colorize(ansiGreen, "● Archive"))
		w.WriteByte('\n')
		writeMetricRow(w, []metric{
			{"workspaces", shortValue(activity["total_workspaces"]), ansiGreen},
			{"channels", shortValue(activity["total_channels"]), ansiGreen},
			{"users", shortValue(activity["total_users"]), ansiGreen},
			{"messages", shortValue(activity["total_messages"]), ansiGreen},
		})
		writeMetricRow(w, []metric{
			{"drafts", shortValue(activity["draft_messages"]), ansiYellow},
			{"edited", shortValue(activity["edited_messages"]), ansiYellow},
			{"deleted", shortValue(activity["deleted_messages"]), ansiYellow},
		})
		if latest := shortValue(activity["latest_message_at"]); latest != "-" {
			w.WriteString("  latest msg   ")
			w.WriteString(latest)
			w.WriteByte('\n')
		}
		if windows, ok := activity["windows"].([]any); ok && len(windows) > 0 {
			w.WriteByte('\n')
			w.WriteString(colorize(ansiCyan, "Windows"))
			w.WriteByte('\n')
			for _, item := range windows {
				window, ok := item.(map[string]any)
				if !ok {
					continue
				}
				w.WriteString("  • ")
				w.WriteString(shortValue(window["label"]))
				w.WriteString("  messages=")
				w.WriteString(shortValue(window["messages"]))
				w.WriteString(" authors=")
				w.WriteString(shortValue(window["active_authors"]))
				w.WriteString(" channels=")
				w.WriteString(shortValue(window["active_channels"]))
				w.WriteByte('\n')
			}
		}
	}
	if shareState, ok := report["share"].(map[string]any); ok {
		renderShareBlock(w, shareState, false)
	}
	return true
}

func renderDigestBlock(w *strings.Builder, value any) bool {
	digest, ok := value.(map[string]any)
	if !ok {
		return false
	}
	writeTitle(w, "DIGEST")

	w.WriteString(colorize(ansiGreen, "● Window"))
	w.WriteByte('\n')
	label := shortValue(digest["window_label"])
	since := shortValue(digest["since"])
	until := shortValue(digest["until"])
	w.WriteString("  window       ")
	w.WriteString(label)
	w.WriteByte('\n')
	w.WriteString("  range        ")
	w.WriteString(since)
	w.WriteString(colorize(ansiDim, " → "))
	w.WriteString(until)
	w.WriteByte('\n')
	if ws := shortValue(digest["workspace"]); ws != "-" {
		w.WriteString("  workspace    ")
		w.WriteString(ws)
		w.WriteByte('\n')
	}
	if ch := shortValue(digest["channel"]); ch != "-" {
		w.WriteString("  channel      ")
		w.WriteString(ch)
		w.WriteByte('\n')
	}

	if totals, ok := digest["totals"].(map[string]any); ok {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiCyan, "Totals"))
		w.WriteByte('\n')
		writeMetricRow(w, []metric{
			{"messages", shortValue(totals["messages"]), ansiGreen},
			{"threads", shortValue(totals["threads"]), ansiGreen},
			{"channels", shortValue(totals["channels"]), ansiGreen},
			{"authors", shortValue(totals["active_authors"]), ansiGreen},
		})
	}

	channels, _ := digest["channels"].([]any)
	w.WriteByte('\n')
	w.WriteString(colorize(ansiCyan, "Channels"))
	w.WriteByte('\n')
	if len(channels) == 0 {
		w.WriteString(colorize(ansiDim, "  no activity in window"))
		w.WriteByte('\n')
		return true
	}
	for _, item := range channels {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := shortValue(row["channel_name"])
		if name == "-" {
			name = shortValue(row["channel_id"])
		}
		w.WriteString("  • ")
		w.WriteString(colorize(ansiBold, name))
		if kind := shortValue(row["kind"]); kind != "-" && kind != "" {
			w.WriteString(colorize(ansiDim, " ("+kind+")"))
		}
		w.WriteByte('\n')
		w.WriteString("      messages=")
		w.WriteString(shortValue(row["messages"]))
		w.WriteString(" threads=")
		w.WriteString(shortValue(row["threads"]))
		w.WriteString(" authors=")
		w.WriteString(shortValue(row["active_authors"]))
		w.WriteByte('\n')
		if posters, ok := row["top_posters"].([]any); ok && len(posters) > 0 {
			w.WriteString("      top posters  ")
			w.WriteString(joinRankedCounts(posters))
			w.WriteByte('\n')
		}
		if mentions, ok := row["top_mentions"].([]any); ok && len(mentions) > 0 {
			w.WriteString("      top mentions ")
			w.WriteString(joinRankedCounts(mentions))
			w.WriteByte('\n')
		}
	}
	return true
}

func renderAnalyticsQuietBlock(w *strings.Builder, value any) bool {
	quiet, ok := value.(map[string]any)
	if !ok {
		return false
	}
	writeTitle(w, "ANALYTICS QUIET")

	w.WriteString(colorize(ansiGreen, "● Window"))
	w.WriteByte('\n')
	w.WriteString("  since        ")
	w.WriteString(shortValue(quiet["since"]))
	w.WriteByte('\n')
	w.WriteString("  until        ")
	w.WriteString(shortValue(quiet["until"]))
	w.WriteByte('\n')
	if ws := shortValue(quiet["workspace"]); ws != "-" {
		w.WriteString("  workspace    ")
		w.WriteString(ws)
		w.WriteByte('\n')
	}

	if totals, ok := quiet["totals"].(map[string]any); ok {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiCyan, "Totals"))
		w.WriteByte('\n')
		writeMetricRow(w, []metric{
			{"channels", shortValue(totals["channels"]), ansiGreen},
		})
	}

	channels, _ := quiet["channels"].([]any)
	w.WriteByte('\n')
	w.WriteString(colorize(ansiCyan, "Channels"))
	w.WriteByte('\n')
	if len(channels) == 0 {
		w.WriteString(colorize(ansiDim, "  no quiet channels in window"))
		w.WriteByte('\n')
		return true
	}
	rows := make([]map[string]any, 0, len(channels))
	for _, item := range channels {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lastMessage := shortValue(row["last_message"])
		if lastMessage == "-" {
			lastMessage = "never"
		}
		rows = append(rows, map[string]any{
			"channel":      shortValue(row["channel_name"]),
			"kind":         shortValue(row["kind"]),
			"last_message": lastMessage,
			"days_silent":  shortValue(row["days_silent"]),
		})
	}
	renderTable(w, rows, 1)
	return true
}

func renderAnalyticsTrendsBlock(w *strings.Builder, value any) bool {
	trends, ok := value.(map[string]any)
	if !ok {
		return false
	}
	writeTitle(w, "ANALYTICS TRENDS")

	w.WriteString(colorize(ansiGreen, "● Window"))
	w.WriteByte('\n')
	w.WriteString("  weeks        ")
	w.WriteString(shortValue(trends["weeks"]))
	w.WriteByte('\n')
	w.WriteString("  since        ")
	w.WriteString(shortValue(trends["since"]))
	w.WriteByte('\n')
	w.WriteString("  until        ")
	w.WriteString(shortValue(trends["until"]))
	w.WriteByte('\n')
	if ws := shortValue(trends["workspace"]); ws != "-" {
		w.WriteString("  workspace    ")
		w.WriteString(ws)
		w.WriteByte('\n')
	}
	if ch := shortValue(trends["channel"]); ch != "-" {
		w.WriteString("  channel      ")
		w.WriteString(ch)
		w.WriteByte('\n')
	}

	rows, _ := trends["rows"].([]any)
	w.WriteByte('\n')
	w.WriteString(colorize(ansiCyan, "Weekly counts"))
	w.WriteByte('\n')
	if len(rows) == 0 {
		w.WriteString(colorize(ansiDim, "  no channel activity in window"))
		w.WriteByte('\n')
		return true
	}

	tableRows := make([]map[string]any, 0)
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		channelName := shortValue(row["channel_name"])
		if channelName == "-" {
			channelName = shortValue(row["channel_id"])
		}
		weekly, _ := row["weekly"].([]any)
		for _, weeklyItem := range weekly {
			week, ok := weeklyItem.(map[string]any)
			if !ok {
				continue
			}
			tableRows = append(tableRows, map[string]any{
				"channel":    channelName,
				"kind":       shortValue(row["kind"]),
				"week_start": shortValue(week["week_start"]),
				"messages":   shortValue(week["messages"]),
			})
		}
	}
	renderTable(w, tableRows, 1)
	return true
}

func joinRankedCounts(items []any) string {
	var parts []string
	for _, item := range items {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := shortValue(row["name"])
		count := shortValue(row["count"])
		parts = append(parts, fmt.Sprintf("%s (%s)", name, count))
	}
	return strings.Join(parts, ", ")
}
