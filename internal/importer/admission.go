package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/slack-go/slack"
)

// Prepared retains catalog evidence and both locator candidates before any
// archive is opened. Scan chooses the legacy name-first branch exactly once.
type Prepared struct {
	export     *Export
	workspace  string
	channels   []ChannelInfo
	users      []slack.User
	candidates [][][]messageFile
	selected   [][]messageFile
	scanned    bool
	omittedDM  int
	strict     bool
}

func (p *Prepared) Channels() []ChannelInfo { return p.channels }
func (p *Prepared) Users() []slack.User     { return p.users }
func (p *Prepared) OmittedDM() int          { return p.omittedDM }

// Prepare qualifies the catalog and reserves physical ownership without reading
// message bodies, including bodies excluded by the DM policy.
func (e *Export) Prepare(workspace string, policy admission.DMPolicy) (*Prepared, error) {
	p := &Prepared{export: e, workspace: workspace, strict: policy == admission.Exclude}
	all, present, err := e.catalogs(policy == admission.Exclude)
	if err != nil {
		return nil, err
	}
	if policy == admission.Exclude && !present {
		return nil, errors.New("unsupported export: include_dms=false requires a workspace JSON export with conversation catalogs")
	}

	veto := map[string]bool{}
	if policy == admission.Exclude {
		for _, channel := range all {
			raw, err := catalogFields(channel.RawJSON)
			if err != nil {
				return nil, err
			}
			veto[channel.ID] = veto[channel.ID] || channel.Kind == "im" || channel.Kind == "mpim" ||
				string(raw["is_im"]) == "true" || string(raw["is_mpim"]) == "true"
		}
	}
	// Excluded identities still own every name and ID locator. Removing them
	// first would allow an admitted channel to consume their message files.
	inventory, err := e.reserveLocators(all, policy == admission.Exclude)
	if err != nil {
		return nil, err
	}
	kinds := map[string]string{}
	seen := map[string]bool{}
	for _, channel := range all {
		if veto[channel.ID] {
			if !seen[channel.ID] {
				p.omittedDM++
				seen[channel.ID] = true
			}
			continue
		}
		if policy == admission.Exclude {
			kind, private, err := exportKind(channel)
			if err != nil {
				return nil, err
			}
			channel.Kind, channel.IsPrivate = kind, private
			if prior, ok := kinds[channel.ID]; ok && prior != kind {
				return nil, errors.New("conflicting export conversation classifications")
			}
			kinds[channel.ID] = kind
		}
		var raw map[string]any
		if err := json.Unmarshal(channel.RawJSON, &raw); err != nil {
			return nil, errors.New("invalid export catalog record")
		}
		if err := validateIdentity(raw, channel.ID, workspace); err != nil {
			return nil, err
		}
		for key, value := range raw {
			if latest, ok := value.(map[string]any); ok && strings.EqualFold(key, "latest") {
				if err := validateIdentity(latest, channel.ID, workspace); err != nil {
					return nil, err
				}
			}
		}
		p.channels = append(p.channels, channel)
		candidates := [][]messageFile{inventory[channel.Name]}
		if channel.Name != channel.ID {
			candidates = append(candidates, inventory[channel.ID])
		}
		p.candidates = append(p.candidates, candidates)
	}
	if policy == admission.Exclude && len(p.channels) == 0 {
		return p, nil
	}
	p.users, err = e.Users()
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Strict duplicate validation precedes this projection, so differently cased
// type keys cannot overwrite each other or disagree with the DM veto.
func catalogFields(blob []byte) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(blob, &raw); err != nil {
		return nil, errors.New("invalid export catalog record")
	}
	fields := make(map[string]json.RawMessage, len(raw))
	for key, value := range raw {
		fields[canonicalCatalogKey(key)] = value
	}
	return fields, nil
}

func exportKind(channel ChannelInfo) (string, bool, error) {
	raw, err := catalogFields(channel.RawJSON)
	if err != nil {
		return "", false, err
	}
	flags := map[string]bool{}
	native := false
	for _, key := range []string{"is_channel", "is_group", "is_im", "is_mpim", "is_private"} {
		value, exists := raw[key]
		if !exists {
			continue
		}
		if key != "is_private" {
			native = true
		}
		if string(value) != "true" && string(value) != "false" {
			return "", false, errors.New("invalid export conversation type flags; include_dms=false requires workspace JSON conversation evidence")
		}
		flags[key] = string(value) == "true"
	}
	private := channel.Kind == "private"
	if value, present := flags["is_private"]; present {
		private = value
	}
	if !native {
		if private != (channel.Kind == "private") {
			return "", false, errors.New("conflicting export catalog privacy")
		}
		return channel.Kind, private, nil
	}
	kind := (admission.NativeFlags{
		IsChannel: flags["is_channel"], IsGroup: flags["is_group"],
		IsIM: flags["is_im"], IsMPIM: flags["is_mpim"], IsPrivate: private,
	}).Kind()
	if kind != admission.PublicChannel && kind != admission.PrivateChannel {
		return "", false, errors.New("unqualified export conversation type; include_dms=false requires workspace JSON conversation evidence")
	}
	if kind == admission.PrivateChannel {
		return "private", true, nil
	}
	return "public", false, nil
}

// Only conversation ownership fields and supported retained message wrappers
// participate. Author teams, mentions, attachments and shared-file references do not.
func validateIdentity(raw map[string]any, channel, workspace string) error {
	// Check every spelling, rather than folding into a map and losing a conflict.
	for key, value := range raw {
		for _, field := range []string{"channel", "channel_id", "conversation", "conversation_id", "context_team_id"} {
			if !strings.EqualFold(key, field) || value == nil {
				continue
			}
			id, ok := value.(string)
			if !ok {
				return errors.New("invalid export conversation identity")
			}
			expected := channel
			if field == "context_team_id" {
				expected = workspace
			}
			if id != "" && id != expected {
				return errors.New("export conversation identity does not match its catalog or workspace")
			}
		}
		for _, field := range []string{"message", "previous_message", "root", "previous"} {
			if nested, ok := value.(map[string]any); ok && strings.EqualFold(key, field) {
				if err := validateIdentity(nested, channel, workspace); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Scan is the first body pass. All identities are checked even when a row has
// no timestamp or will later lose a source-priority comparison.
func (p *Prepared) Scan(ctx context.Context, visit func(ChannelInfo, MessageEnvelope) error) error {
	if p.scanned {
		return errors.New("export preparation was already scanned")
	}
	p.selected = make([][]messageFile, len(p.channels))
	for i, channel := range p.channels {
		for _, candidate := range p.candidates[i] {
			count := 0
			for _, file := range candidate {
				if err := ctx.Err(); err != nil {
					return err
				}
				blob, err := p.export.readMessageFile(file)
				if err != nil {
					return err
				}
				file.digest = messageDigest(blob)
				if p.strict && len(bytes.TrimSpace(blob)) > 0 {
					if err := validateUniqueJSONKeys(blob, false); err != nil {
						return err
					}
				}
				rows, err := decodeMessages(blob)
				if err != nil {
					return err
				}
				p.selected[i] = append(p.selected[i], file)
				for _, raw := range rows {
					if err := validateIdentity(raw, channel.ID, p.workspace); err != nil {
						return err
					}
					if err := visit(channel, MessageEnvelope{Date: strings.TrimSuffix(file.baseName(), ".json"), Raw: raw}); err != nil {
						return err
					}
					count++
				}
			}
			if count > 0 {
				break
			}
			// Empty name files still form part of the plan: verify them on execution,
			// but never discover a new fallback after preparation.
		}
	}
	p.scanned = true
	return nil
}
