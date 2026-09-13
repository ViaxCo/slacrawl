package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/report"
)

func (a *App) runDigest(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, configErr := loadConfig(configPath)
	if configErr != nil {
		cfg = config.Default()
	}
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	since := fs.String("since", "7d", "lookback window, e.g. 24h, 7d, 30d")
	workspaceID := fs.String("workspace", "", "workspace id")
	channel := fs.String("channel", "", "channel id or name")
	topN := fs.Int("top-n", 1, "number of top posters and mentions per channel")
	formatFlag := fs.String("format", string(format), "output format: text|json|log")
	jsonOut := fs.Bool("json", false, "json output")
	if err := a.parseCommandFlags(fs, args); err != nil {
		return err
	}
	if configErr != nil {
		return configErr
	}
	lookback, err := parseLookback(*since)
	if err != nil {
		return fmt.Errorf("parse --since: %w", err)
	}
	outputFormat, err := resolveOutputFormat(*formatFlag, *jsonOut)
	if err != nil {
		return err
	}
	st, err := a.openReadableStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	digest, err := report.BuildDigest(ctx, st, report.DigestOptions{
		Now:         a.nowUTC(),
		Since:       lookback,
		WorkspaceID: coalesce(*workspaceID, cfg.WorkspaceID),
		Channel:     *channel,
		TopN:        *topN,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("Digest", digest, outputFormat, true)
}

// parseLookback accepts Go durations (72h) plus the shorthand Nd for N days.
func parseLookback(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("empty duration")
	}
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day count: %w", err)
		}
		if days < 0 {
			return 0, errors.New("negative duration")
		}
		const day = 24 * time.Hour
		if days > int64((1<<63-1)/day) {
			return 0, errors.New("duration exceeds supported range")
		}
		return time.Duration(days) * day, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, errors.New("negative duration")
	}
	return d, nil
}

func (a *App) runReport(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	if err := a.parseCommandFlags(fs, args); err != nil {
		return err
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	st, err := a.openReadableStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	activity, err := report.Build(ctx, st, report.Options{Now: a.nowUTC()})
	if err != nil {
		return err
	}
	shareState, err := a.buildShareState(ctx, cfg, st)
	if err != nil {
		return err
	}
	return a.writeOutput("Report", map[string]any{
		"activity": activity,
		"share":    shareState,
	}, format, true)
}
