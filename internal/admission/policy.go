package admission

type Kind uint8

const (
	Unknown Kind = iota
	PublicChannel
	PrivateChannel
	IM
	MPIM
)

type DMPolicy uint8

const (
	Default DMPolicy = iota
	Exclude
	Include
)

func FromConfig(value *bool) DMPolicy {
	if value == nil {
		return Default
	}
	if *value {
		return Include
	}
	return Exclude
}

// Enabled resolves acquisition defaults without discarding an explicit exclusion.
// Sources have different defaults; keep the policy unresolved until that boundary.
func (p DMPolicy) Enabled(sourceDefault bool) bool {
	return p == Include || (p == Default && sourceDefault)
}

func (p DMPolicy) Allows(kind Kind) bool {
	return p != Exclude || kind == PublicChannel || kind == PrivateChannel
}

// NativeFlags are Slack conversation facts, not inferred names, prefixes, or
// list-request filters. DM flags take exclusion precedence over channel flags.
type NativeFlags struct {
	IsIM      bool `json:"is_im"`
	IsMPIM    bool `json:"is_mpim"`
	IsChannel bool `json:"is_channel"`
	IsGroup   bool `json:"is_group"`
	IsPrivate bool `json:"is_private"`
}

func (f NativeFlags) Kind() Kind {
	switch {
	case f.IsIM:
		return IM
	case f.IsMPIM:
		return MPIM
	case f.IsChannel && !f.IsGroup:
		if f.IsPrivate {
			return PrivateChannel
		}
		return PublicChannel
	case !f.IsChannel && f.IsGroup && f.IsPrivate:
		return PrivateChannel
	default:
		return Unknown
	}
}
