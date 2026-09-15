package admission

import "testing"

func TestDMPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  *bool
		policy DMPolicy
	}{
		{"omitted", nil, Default},
		{"false", new(false), Exclude},
		{"true", new(true), Include},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := FromConfig(tc.value)
			if policy != tc.policy {
				t.Fatalf("policy = %v, want %v", policy, tc.policy)
			}
			for _, sourceDefault := range []bool{false, true} {
				want := sourceDefault
				if tc.value != nil {
					want = *tc.value
				}
				if policy.Enabled(sourceDefault) != want {
					t.Errorf("enabled(default=%v) = %v, want %v", sourceDefault, policy.Enabled(sourceDefault), want)
				}
			}
			for _, kind := range []Kind{Unknown, PublicChannel, PrivateChannel, IM, MPIM} {
				want := tc.policy != Exclude || kind == PublicChannel || kind == PrivateChannel
				if policy.Allows(kind) != want {
					t.Errorf("allows(%v) = %v, want %v", kind, policy.Allows(kind), want)
				}
			}
		})
	}
}
