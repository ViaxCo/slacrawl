package cli

import (
	"strings"
)

func renderDoctorBlock(w *strings.Builder, value any) bool {
	report, ok := value.(map[string]any)
	if !ok {
		return false
	}

	writeTitle(w, "DOCTOR")
	w.WriteString(colorize(ansiGreen, "● Ready checks"))
	w.WriteByte('\n')

	writeCheck(w, "config", report["config_path"] != nil, shortValue(report["config_path"]))
	writeCheck(w, "database", report["database_path"] != nil, shortValue(report["database_path"]))
	writeCheck(w, "fts5", truthy(report["fts_available"]), "sqlite virtual table available")

	if slackAPI, ok := report["slack_api"].(map[string]any); ok {
		writeCheck(w, "bot token", truthy(slackAPI["bot_configured"]), teamLabel(slackAPI))
		writeCheck(w, "app tail", truthy(slackAPI["app_tail_available"]), ternary(truthy(slackAPI["app_tail_available"]), "socket mode available", "app token missing"))
		coverage := shortValue(slackAPI["thread_coverage"])
		writeCheck(w, "thread coverage", coverage == "full", ternary(coverage == "full", "full historical replies", "partial without user auth"))
		if truthy(slackAPI["dms_included"]) {
			missing := shortValue(slackAPI["dms_missing_scope"])
			if missing == "" {
				writeCheck(w, "dms and mpims", true, "user token covers DMs and MPIMs")
			} else {
				writeCheck(w, "dms and mpims", false, "missing scope: "+missing)
			}
		} else {
			writeCheck(w, "dms and mpims", false, "disabled (set sync.include_dms with a user token)")
		}
	}
	if desktop, ok := report["desktop_source"].(map[string]any); ok {
		writeCheck(w, "desktop cache", truthy(desktop["available"]), shortValue(desktop["path"]))
	}

	if status, ok := report["status"].(map[string]any); ok {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiCyan, "Snapshot"))
		w.WriteByte('\n')
		writeMetricRow(w, []metric{
			{"workspaces", shortValue(status["workspaces"]), ansiGreen},
			{"channels", shortValue(status["channels"]), ansiGreen},
			{"users", shortValue(status["users"]), ansiGreen},
			{"messages", shortValue(status["messages"]), ansiGreen},
		})
		w.WriteString("  last sync    ")
		w.WriteString(shortValue(status["last_sync_at"]))
		w.WriteByte('\n')
		w.WriteString("  thread state ")
		w.WriteString(shortValue(status["thread_state"]))
		w.WriteByte('\n')
	}
	if profile, ok := report["archive_profile"].(map[string]any); ok {
		renderArchiveProfileBlock(w, profile)
	}
	if shareState, ok := report["share"].(map[string]any); ok {
		renderShareBlock(w, shareState, true)
	}

	if skips, ok := report["api_channel_skips"].([]any); ok && len(skips) > 0 {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiYellow, "API channel skips"))
		w.WriteByte('\n')
		for _, item := range skips {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			w.WriteString("  ! ")
			w.WriteString(shortValue(row["entity_id"]))
			w.WriteString("  ")
			w.WriteString(shortValue(row["value"]))
			w.WriteByte('\n')
		}
	}

	if tail, ok := report["tail_state"].([]any); ok && len(tail) > 0 {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiCyan, "Tail state"))
		w.WriteByte('\n')
		for _, item := range tail {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			w.WriteString("  • ")
			w.WriteString(shortValue(row["entity_type"]))
			w.WriteString(" ")
			w.WriteString(shortValue(row["entity_id"]))
			w.WriteString(" ")
			w.WriteString(shortValue(row["value"]))
			w.WriteByte('\n')
		}
	}

	return true
}

func renderStatusBlock(w *strings.Builder, value any) bool {
	report, ok := value.(map[string]any)
	if !ok {
		return false
	}

	writeTitle(w, "STATUS")
	w.WriteString(colorize(ansiGreen, "● Archive"))
	w.WriteByte('\n')
	writeMetricRow(w, []metric{
		{"workspaces", shortValue(report["workspaces"]), ansiGreen},
		{"channels", shortValue(report["channels"]), ansiGreen},
		{"users", shortValue(report["users"]), ansiGreen},
		{"messages", shortValue(report["messages"]), ansiGreen},
	})
	w.WriteString("  last sync    ")
	w.WriteString(shortValue(report["last_sync_at"]))
	w.WriteByte('\n')
	w.WriteString("  thread state ")
	w.WriteString(shortValue(report["thread_state"]))
	w.WriteByte('\n')

	if profile, ok := report["archive_profile"].(map[string]any); ok {
		renderArchiveProfileBlock(w, profile)
	}

	if shareState, ok := report["share"].(map[string]any); ok {
		renderShareBlock(w, shareState, true)
	}

	return true
}

func renderSyncBlock(w *strings.Builder, title string, value any) bool {
	report, ok := value.(map[string]any)
	if !ok {
		return false
	}

	writeTitle(w, strings.ToUpper(title))
	w.WriteString(colorize(ansiGreen, "● Completed"))
	w.WriteString(colorize(ansiDim, "  local state refreshed"))
	w.WriteByte('\n')

	if status, ok := report["status"].(map[string]any); ok {
		w.WriteByte('\n')
		w.WriteString(colorize(ansiCyan, "State"))
		w.WriteByte('\n')
		writeMetricRow(w, []metric{
			{"workspaces", shortValue(status["workspaces"]), ansiGreen},
			{"channels", shortValue(status["channels"]), ansiGreen},
			{"users", shortValue(status["users"]), ansiGreen},
			{"messages", shortValue(status["messages"]), ansiGreen},
		})
		if threadState := shortValue(status["thread_state"]); threadState != "" {
			w.WriteString("  thread state ")
			w.WriteString(threadState)
			w.WriteByte('\n')
		}
	}
	if summary, ok := report["summary"].(map[string]any); ok {
		if desktop, ok := summary["desktop"].(map[string]any); ok {
			w.WriteByte('\n')
			w.WriteString(colorize(ansiCyan, "Desktop"))
			w.WriteByte('\n')
			writeCheck(w, "available", truthy(desktop["available"]), shortValue(desktop["path"]))
			if root, ok := desktop["summary"].(map[string]any); ok {
				writeMetricRow(w, []metric{
					{"workspaces", shortValue(root["workspace_count"]), ansiBlue},
					{"teams", shortValue(root["teams_count"]), ansiBlue},
					{"downloads", shortValue(root["download_item_count"]), ansiBlue},
				})
			}
			if local, ok := desktop["local_storage"].(map[string]any); ok {
				writeMetricRow(w, []metric{
					{"drafts", shortValue(local["draft_count"]), ansiYellow},
					{"markers", shortValue(local["read_marker_count"]), ansiYellow},
					{"recent", shortValue(local["recent_channel_count"]), ansiYellow},
				})
			}
		}
	}
	if profile, ok := report["archive_profile"].(map[string]any); ok {
		renderArchiveProfileBlock(w, profile)
	}
	return true
}

func renderArchiveProfileBlock(w *strings.Builder, profile map[string]any) {
	w.WriteByte('\n')
	w.WriteString(colorize(ansiCyan, "Archive profile"))
	w.WriteByte('\n')
	w.WriteString("  mode         ")
	w.WriteString(shortValue(profile["mode"]))
	w.WriteByte('\n')
	sources, ok := profile["sources"].([]any)
	if !ok || len(sources) == 0 {
		return
	}
	for _, item := range sources {
		source, ok := item.(map[string]any)
		if !ok {
			continue
		}
		detail := []string{}
		if truthy(source["configured"]) {
			detail = append(detail, "configured")
		}
		if last := shortValue(source["last_seen_at"]); last != "" {
			detail = append(detail, "last "+last)
		}
		if messages := shortValue(source["messages"]); messages != "" && messages != "0" {
			detail = append(detail, messages+" msgs")
		}
		writeCheck(w, shortValue(source["name"]), truthy(source["enabled"]), strings.Join(detail, ", "))
	}
}

func renderShareBlock(w *strings.Builder, shareState map[string]any, includeManifest bool) {
	w.WriteByte('\n')
	w.WriteString(colorize(ansiCyan, "Git share"))
	w.WriteByte('\n')

	enabled := truthy(shareState["enabled"])
	detail := shortValue(shareState["remote"])
	if detail == "-" {
		detail = ternary(enabled, shortValue(shareState["repo_path"]), "not configured")
	}
	writeCheck(w, "enabled", enabled, detail)

	if !enabled {
		return
	}

	autoUpdate := truthy(shareState["auto_update"])
	staleAfter := shortValue(shareState["stale_after"])
	if staleAfter == "-" {
		staleAfter = ternary(autoUpdate, "enabled", "disabled")
	}
	writeCheck(w, "auto update", autoUpdate, staleAfter)

	if imported := shortValue(shareState["last_import_at"]); imported != "-" {
		w.WriteString("  last import  ")
		w.WriteString(imported)
		w.WriteByte('\n')
	}
	if includeManifest {
		if manifest := shortValue(shareState["last_manifest_generated_at"]); manifest != "-" {
			w.WriteString("  manifest     ")
			w.WriteString(manifest)
			w.WriteByte('\n')
		}
	}
	w.WriteString("  refresh due  ")
	w.WriteString(ternary(truthy(shareState["needs_import"]), "yes", "no"))
	w.WriteByte('\n')
}
