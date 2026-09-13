package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/media"
	"github.com/openclaw/slacrawl/internal/store"
)

func (a *App) runFiles(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	if len(args) > 0 && args[0] == "fetch" {
		return a.runFilesFetch(ctx, configPath, args[1:], format)
	}
	if hasHelpArg(args) {
		printFilesUsage(a.Stdout)
		return nil
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	opts, err := a.parseFileListOptions(args, cfg, 50)
	if err != nil {
		return err
	}
	st, err := a.openReadableStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	queryOpts := opts
	if opts.MissingOnly {
		queryOpts.MissingOnly = false
		queryOpts.Limit = 0
	}
	rows, err := st.Files(ctx, queryOpts)
	if err != nil {
		return err
	}
	if opts.MissingOnly {
		limit := opts.Limit
		rows = filterMissingFileMedia(cfg.CacheDir, rows)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
	}
	return a.writeOutput("Files", rows, format, false)
}

func (a *App) runFilesFetch(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	if hasHelpArg(args) {
		printFilesUsage(a.Stdout)
		return nil
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	listArgs := stripKnownFlags(args, map[string]bool{"force": false, "max-bytes": true})
	opts, err := a.parseFileListOptions(listArgs, cfg, 50)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("files fetch", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	force := fs.Bool("force", false, "redownload cached files")
	maxBytes := fs.Int64("max-bytes", cfg.Sync.MaxFileBytes, "per-file download cap")
	if err := parseOnlyKnown(fs, args, fileListFlagNames()); err != nil {
		return err
	}
	if *maxBytes <= 0 {
		return errors.New("--max-bytes must be positive")
	}
	st, err := a.openAutoUpdatingStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	stats, err := a.fetchFiles(ctx, cfg, st, opts, *maxBytes, *force)
	if err != nil {
		return err
	}
	return a.writeOutput("Files", stats, format, false)
}

func (a *App) parseFileListOptions(args []string, cfg config.Config, defaultLimit int) (store.FileListOptions, error) {
	fs := flag.NewFlagSet("files", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	workspaceID := fs.String("workspace", "", "workspace id")
	channelID := fs.String("channel", "", "channel id")
	userID := fs.String("user", "", "user id")
	fileID := fs.String("file", "", "file id")
	filename := fs.String("filename", "", "filename filter")
	contentType := fs.String("type", "", "content type or filetype")
	since := fs.String("since", "", "RFC3339 lower bound")
	before := fs.String("before", "", "RFC3339 upper bound")
	limit := fs.Int("limit", defaultLimit, "row limit")
	all := fs.Bool("all", false, "return all rows")
	missing := fs.Bool("missing", false, "only files without cached media")
	if err := fs.Parse(args); err != nil {
		return store.FileListOptions{}, err
	}
	if fs.NArg() != 0 {
		return store.FileListOptions{}, errors.New("files takes flags only")
	}
	if *limit < 0 {
		return store.FileListOptions{}, errors.New("--limit must be >= 0")
	}
	var sinceTime, beforeTime time.Time
	var err error
	if strings.TrimSpace(*since) != "" {
		sinceTime, err = time.Parse(time.RFC3339, *since)
		if err != nil {
			return store.FileListOptions{}, fmt.Errorf("invalid --since: %w", err)
		}
	}
	if strings.TrimSpace(*before) != "" {
		beforeTime, err = time.Parse(time.RFC3339, *before)
		if err != nil {
			return store.FileListOptions{}, fmt.Errorf("invalid --before: %w", err)
		}
	}
	if *all {
		*limit = 0
	}
	return store.FileListOptions{
		WorkspaceID: coalesce(*workspaceID, cfg.WorkspaceID),
		ChannelID:   strings.TrimSpace(*channelID),
		UserID:      strings.TrimSpace(*userID),
		FileID:      strings.TrimSpace(*fileID),
		Filename:    strings.TrimSpace(*filename),
		ContentType: strings.TrimSpace(*contentType),
		Since:       sinceTime,
		Before:      beforeTime,
		Limit:       *limit,
		MissingOnly: *missing,
	}, nil
}

func filterMissingFileMedia(cacheDir string, rows []store.FileRow) []store.FileRow {
	out := rows[:0]
	for _, row := range rows {
		if row.MediaPath == "" {
			out = append(out, row)
			continue
		}
		path, err := media.LocalPath(cacheDir, row.MediaPath)
		if err != nil {
			out = append(out, row)
			continue
		}
		if _, err := os.Stat(path); err != nil {
			out = append(out, row)
		}
	}
	return out
}

func (a *App) fetchFiles(ctx context.Context, cfg config.Config, st *store.Store, opts store.FileListOptions, maxBytes int64, force bool) (media.FetchStats, error) {
	workspaceID := strings.TrimSpace(opts.WorkspaceID)
	if workspaceID != "" {
		return media.Fetch(ctx, st, media.FetchOptions{
			CacheDir:     cfg.CacheDir,
			List:         opts,
			MaxBytes:     maxBytes,
			Force:        force,
			Token:        mediaToken(cfg, workspaceID),
			HTTPClient:   a.httpClient,
			StatusUpdate: true,
			Now:          a.nowUTC,
		})
	}
	targets := resolveWorkspaceTargets(cfg, "")
	if len(targets) == 0 {
		return media.Fetch(ctx, st, media.FetchOptions{
			CacheDir:     cfg.CacheDir,
			List:         opts,
			MaxBytes:     maxBytes,
			Force:        force,
			Token:        mediaToken(cfg, ""),
			HTTPClient:   a.httpClient,
			StatusUpdate: true,
			Now:          a.nowUTC,
		})
	}
	remaining := opts.Limit
	var total media.FetchStats
	for _, target := range targets {
		batch := opts
		batch.WorkspaceID = target
		if remaining > 0 {
			batch.Limit = remaining
		}
		stats, err := media.Fetch(ctx, st, media.FetchOptions{
			CacheDir:     cfg.CacheDir,
			List:         batch,
			MaxBytes:     maxBytes,
			Force:        force,
			Token:        mediaToken(cfg, target),
			HTTPClient:   a.httpClient,
			StatusUpdate: true,
			Now:          a.nowUTC,
		})
		if err != nil {
			return media.FetchStats{}, err
		}
		addMediaStats(&total, stats)
		if remaining > 0 {
			remaining -= stats.Files
			if remaining <= 0 {
				break
			}
		}
	}
	return total, nil
}

func (a *App) fetchMediaForSync(ctx context.Context, cfg config.Config, st *store.Store, workspaceID string, channels []string) (media.FetchStats, error) {
	targets := resolveWorkspaceTargets(cfg, workspaceID)
	if len(targets) == 0 {
		targets = []string{workspaceID}
	}
	var total media.FetchStats
	for _, target := range targets {
		channelTargets := channels
		if len(channelTargets) == 0 {
			channelTargets = []string{""}
		}
		for _, channelID := range channelTargets {
			stats, err := media.Fetch(ctx, st, media.FetchOptions{
				CacheDir:     cfg.CacheDir,
				List:         store.FileListOptions{WorkspaceID: target, ChannelID: channelID},
				MaxBytes:     cfg.Sync.MaxFileBytes,
				Token:        mediaToken(cfg, target),
				StatusUpdate: true,
				Now:          a.nowUTC,
			})
			if err != nil {
				return media.FetchStats{}, err
			}
			addMediaStats(&total, stats)
		}
	}
	return total, nil
}

func addMediaStats(total *media.FetchStats, stats media.FetchStats) {
	total.Files += stats.Files
	total.Fetched += stats.Fetched
	total.Reused += stats.Reused
	total.Skipped += stats.Skipped
	total.Failed += stats.Failed
	total.Bytes += stats.Bytes
}

func mediaToken(cfg config.Config, workspaceID string) string {
	tokens := cfg.ResolveTokensForWorkspace(workspaceID)
	if tokens.User != "" {
		return tokens.User
	}
	return tokens.Bot
}

func fileListFlagNames() map[string]bool {
	return map[string]bool{
		"workspace": true, "channel": true, "user": true, "file": true,
		"filename": true, "type": true, "since": true, "before": true,
		"limit": true, "all": false, "missing": false,
	}
}

func parseOnlyKnown(fs *flag.FlagSet, args []string, listFlags map[string]bool) error {
	return fs.Parse(stripKnownFlags(args, listFlags))
}

func stripKnownFlags(args []string, strip map[string]bool) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			out = append(out, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if key, _, ok := strings.Cut(name, "="); ok {
			name = key
		}
		if hasValue, ok := strip[name]; ok {
			if hasValue && !strings.Contains(arg, "=") && i+1 < len(args) {
				i++
			}
			continue
		}
		out = append(out, arg)
	}
	return out
}

func printFilesUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `Usage:
  slacrawl files [flags]
  slacrawl files fetch [flags]

Flags:
  -workspace string  workspace id
  -channel string    channel id
  -user string       user id
  -file string       file id
  -filename string   filename/title filter
  -type string       mimetype or filetype filter
  -since string      RFC3339 lower bound
  -before string     RFC3339 upper bound
  -missing           only files without cached media
  -limit int         row limit (default 50)
  -all               return all rows

Fetch flags:
  -force             redownload cached files
  -max-bytes int     per-file download cap
`)
}
