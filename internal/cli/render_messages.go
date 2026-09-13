package cli

import (
	"fmt"
	"strings"
)

func renderMessageListBlock(w *strings.Builder, title string, value any, includeNormalized bool) bool {
	rows, ok := value.([]any)
	if !ok {
		return false
	}

	writeTitle(w, strings.ToUpper(title))
	if len(rows) == 0 {
		w.WriteString(colorize(ansiDim, "no rows"))
		w.WriteByte('\n')
		return true
	}

	for i, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if i > 0 {
			w.WriteByte('\n')
		}
		channel := shortValue(row["channel_name"])
		if channel == "-" {
			channel = shortValue(row["channel_id"])
		}
		if workspace := shortValue(row["workspace_name"]); workspace != "-" {
			channel = workspace + "/" + channel
		}
		user := shortValue(row["user_name"])
		if user == "-" {
			user = shortValue(row["user_id"])
		}
		w.WriteString(colorize(ansiDim, fmt.Sprintf("[%02d] ", i+1)))
		w.WriteString(colorize(ansiCyan, channel))
		if user != "-" {
			w.WriteString(colorize(ansiDim, " by "))
			w.WriteString(colorize(ansiGreen, user))
		}
		if ts := shortValue(row["ts"]); ts != "-" {
			w.WriteString(colorize(ansiDim, " @ "))
			w.WriteString(ts)
		}
		w.WriteByte('\n')
		w.WriteString("     ")
		w.WriteString(trimTo(shortValue(row["text"]), 110))
		w.WriteByte('\n')
		if includeNormalized {
			if normalized := shortValue(row["normalized_text"]); normalized != "-" && normalized != shortValue(row["text"]) {
				w.WriteString("     ")
				w.WriteString(colorize(ansiDim, trimTo(normalized, 110)))
				w.WriteByte('\n')
			}
		}
		if subtype := shortValue(row["subtype"]); subtype != "-" || shortValue(row["thread_ts"]) != "-" {
			w.WriteString("     ")
			if subtype != "-" {
				w.WriteString(colorize(ansiYellow, "subtype="+subtype))
			}
			if threadTS := shortValue(row["thread_ts"]); threadTS != "-" {
				if subtype != "-" {
					w.WriteString(colorize(ansiDim, "  "))
				}
				w.WriteString(colorize(ansiBlue, "thread="+threadTS))
			}
			w.WriteByte('\n')
		}
	}
	return true
}
