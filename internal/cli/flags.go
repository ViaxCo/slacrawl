package cli

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
)

type slacrawlRootArgs struct {
	Config  string   `help:"Config path."`
	Format  string   `default:"text" help:"Output format: text, json, or log."`
	JSON    bool     `name:"json" help:"Compatibility alias for --format json."`
	NoColor bool     `name:"no-color" help:"Disable ANSI color in text output."`
	Version bool     `name:"version" help:"Print version."`
	Args    []string `arg:"" optional:"" passthrough:"partial" name:"command" help:"Command and arguments."`
}

func rootHelpRequested(args []string, valueFlags ...string) bool {
	valueFlagSet := make(map[string]struct{}, len(valueFlags))
	for _, flag := range valueFlags {
		valueFlagSet[flag] = struct{}{}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--help" || arg == "-h" || (arg == "help" && i == len(args)-1) {
			return true
		}
		if !strings.HasPrefix(arg, "-") {
			return false
		}
		if name, ok := strings.CutPrefix(arg, "--"); ok {
			if strings.Contains(name, "=") {
				continue
			}
			if _, ok := valueFlagSet[name]; ok {
				i++
			}
		}
	}
	return false
}

func parseKongArgs(target any, args []string, name string, stdout, stderr io.Writer, options ...kong.Option) error {
	opts := []kong.Option{
		kong.Name(name),
		kong.NoDefaultHelp(),
		kong.Writers(stdout, stderr),
		kong.Exit(func(int) {}),
	}
	opts = append(opts, options...)
	parser, err := kong.New(target, opts...)
	if err != nil {
		return err
	}
	_, err = parser.Parse(args)
	return err
}

func csv(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func mergeStringSlices(values ...[]string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, list := range values {
		for _, value := range list {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			key := strings.ToLower(strings.TrimPrefix(value, "#"))
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, value)
		}
	}
	return out
}

// normalizeSinceTimestamp converts the documented "slack ts or RFC3339" flag
// contract into a slack ts for every backend. Only the MCP path used to
// normalize RFC3339, so the API and desktop paths silently passed the raw
// string through to Slack's `oldest` parameter.
func normalizeSinceTimestamp(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return fmt.Sprintf("%d.%06d", parsed.Unix(), parsed.Nanosecond()/1000), nil
	}
	return "", fmt.Errorf("--since must be a slack timestamp or an RFC3339 time, got %q", value)
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	wasSet := false
	fs.Visit(func(candidate *flag.Flag) {
		if candidate.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func boolPtr(value bool) *bool {
	return &value
}

func isValidChannelKind(kind string) bool {
	switch kind {
	case "im", "mpim", "public_channel", "private_channel":
		return true
	default:
		return false
	}
}

func normalizeChannelKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "public":
		return "public_channel"
	case "private":
		return "private_channel"
	default:
		return strings.TrimSpace(kind)
	}
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		if arg == "help" || arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func normalizeCommandHelp(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

func (a *App) parseCommandFlags(fs *flag.FlagSet, args []string) error {
	var output bytes.Buffer
	fs.SetOutput(&output)
	err := fs.Parse(args)
	if output.Len() == 0 {
		return err
	}

	target := a.Stderr
	if errors.Is(err, flag.ErrHelp) {
		target = a.Stdout
	}
	if target == nil {
		target = io.Discard
	}
	if _, writeErr := io.Copy(target, &output); writeErr != nil {
		return writeErr
	}
	return err
}

func normalizeSingleDashLongFlags(args []string, names ...string) []string {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	out := make([]string, len(args))
	for i, arg := range args {
		if strings.HasPrefix(arg, "--") || !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "-=") {
			out[i] = arg
			continue
		}
		name := strings.TrimPrefix(arg, "-")
		if before, _, ok := strings.Cut(name, "="); ok {
			name = before
		}
		if _, ok := allowed[name]; ok {
			out[i] = "-" + arg
			continue
		}
		out[i] = arg
	}
	return out
}
