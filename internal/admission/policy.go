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
