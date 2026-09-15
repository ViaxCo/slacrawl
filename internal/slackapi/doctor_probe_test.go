package slackapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
)

func TestDoctorDMProbeCatalogFailures(t *testing.T) {
	for _, failure := range []string{"native", "malformed", "absent-ok", "cursor", "rate-limit", "missing-scope", "live-canceled", "live-deadline"} {
		t.Run(failure, func(t *testing.T) {
			catalogCalls, sleeps := 0, 0
			client := NewWithOptions(config.Tokens{User: "fixture-user"}, "https://fixture.invalid/", &http.Client{Transport: primaryOwnerRoundTrip(func(r *http.Request) (*http.Response, error) {
				require.NoError(t, r.ParseForm())
				body := `{"ok":true,"team_id":"T123"}`
				status := http.StatusOK
				header := http.Header{}
				if r.URL.Path != "/auth.test" {
					require.Equal(t, "/conversations.list", r.URL.Path, "catalog failure must stop history sampling")
					require.Equal(t, "im,mpim", r.Form.Get("types"))
					require.Equal(t, "T123", r.Form.Get("team_id"))
					catalogCalls++
					switch failure {
					case "native":
						body = `{"ok":false,"error":"probe-private-canary"}`
					case "malformed":
						body = `{"ok":"probe-private-canary"}`
					case "absent-ok":
						body = `{"channels":[]}`
					case "cursor":
						require.Equal(t, map[bool]string{true: "", false: "probe-private-canary"}[catalogCalls == 1], r.Form.Get("cursor"))
						body = `{"ok":true,"channels":[],"response_metadata":{"next_cursor":"probe-private-canary"}}`
					case "rate-limit":
						status = http.StatusTooManyRequests
						header.Set("Retry-After", "1")
					case "missing-scope":
						body = `{"ok":false,"error":"missing_scope"}`
					case "live-canceled":
						return nil, fmt.Errorf("probe-private-canary: %w", context.Canceled)
					case "live-deadline":
						return nil, fmt.Errorf("probe-private-canary: %w", context.DeadlineExceeded)
					}
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}).WithDMPolicy(admission.Include)
			client.sleep = func(ctx context.Context, delay time.Duration) error {
				sleeps++
				require.Equal(t, time.Second, delay)
				return ctx.Err()
			}
			diag, err := client.Doctor(context.Background())
			require.NoError(t, err)
			wantFailure, wantScope := "catalog_failed", ""
			if failure == "missing-scope" {
				wantFailure, wantScope = "", "im:read,mpim:read"
			}
			require.Equal(t, wantFailure, diag.DMProbeError)
			require.Equal(t, wantScope, diag.DMsMissingScope)
			require.True(t, diag.UserAuthAvailable)
			require.Empty(t, diag.UserAuthError)
			require.True(t, diag.DMsIncluded)
			require.Equal(t, "full", diag.ThreadCoverage)
			require.Empty(t, diag.ThreadCoverageReason)
			expectedCalls := 1
			if failure == "cursor" {
				expectedCalls = 2
			}
			if failure == "rate-limit" {
				expectedCalls = 3
			}
			require.Equal(t, expectedCalls, catalogCalls)
			require.Equal(t, map[bool]int{true: 2, false: 0}[failure == "rate-limit"], sleeps)
			data, err := json.Marshal(diag)
			require.NoError(t, err)
			require.NotContains(t, string(data), "probe-private-canary")
		})
	}
}

func TestDoctorDMProbeHistorySampling(t *testing.T) {
	for _, tc := range []struct{ name, im, mpim, scope, failure string }{
		{"first-fails", "fail", "success", "", "history_failed"},
		{"second-fails", "success", "fail", "", "history_failed"},
		{"scope-then-failure", "scope", "fail", "im:history", "history_failed"},
		{"failure-then-scope", "fail", "scope", "mpim:history", "history_failed"},
		{"both-scopes", "scope", "scope", "im:history,mpim:history", ""},
		{"both-fail", "fail", "fail", "", "history_failed"},
		{"success-flags", "success", "success", "", ""},
		{"paged-catalog", "success", "success", "", ""},
		{"empty", "", "", "", ""},
		{"only-im", "success", "", "", ""},
		{"only-mpim", "", "success", "", ""},
		{"excluded", "success", "success", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var samples []string
			catalogs := 0
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				switch r.URL.Path {
				case "/auth.test":
					return primaryOwnerResponse(r.URL.Path), nil
				case "/conversations.list":
					catalogs++
					if tc.name == "paged-catalog" && form.Get("cursor") == "" {
						return map[string]any{"ok": true, "channels": []any{map[string]any{"id": "D1", "is_im": true}}, "response_metadata": map[string]any{"next_cursor": "second"}}, nil
					}
					if tc.name == "paged-catalog" {
						require.Equal(t, "second", form.Get("cursor"))
					}
					channels := []any{}
					if tc.im != "" {
						channels = append(channels, map[string]any{"id": "D1", "is_im": true}, map[string]any{"id": "D2", "is_im": true})
					}
					if tc.mpim != "" {
						channels = append(channels, map[string]any{"id": "G1", "is_mpim": true}, map[string]any{"id": "G2", "is_mpim": true})
					}
					return map[string]any{"ok": true, "channels": channels}, nil
				case "/conversations.history":
					id := form.Get("channel")
					samples = append(samples, id)
					require.Equal(t, "1", form.Get("limit"))
					require.Empty(t, form.Get("cursor"))
					require.Contains(t, []string{"D1", "G1"}, id, "only one sample per kind")
					outcome := tc.im
					if id == "G1" {
						outcome = tc.mpim
					}
					switch outcome {
					case "scope":
						return map[string]any{"ok": false, "error": "missing_scope"}, nil
					case "fail":
						return map[string]any{"ok": false, "error": "probe-private-canary"}, nil
					default:
						return map[string]any{"ok": true, "messages": []any{}, "has_more": true, "is_limited": true, "response_metadata": map[string]any{"next_cursor": "probe-private-canary"}}, nil
					}
				default:
					t.Fatalf("unexpected method %s", r.URL.Path)
					return nil, nil
				}
			}).WithDMPolicy(admission.Include)
			if tc.name == "excluded" {
				client.WithDMPolicy(admission.Exclude)
			}
			diag, err := client.Doctor(context.Background())
			require.NoError(t, err)
			wantSamples := []string(nil)
			if tc.name != "excluded" {
				if tc.im != "" {
					wantSamples = append(wantSamples, "D1")
				}
				if tc.mpim != "" {
					wantSamples = append(wantSamples, "G1")
				}
			}
			require.Equal(t, wantSamples, samples)
			wantCatalogs := 1
			if tc.name == "excluded" {
				wantCatalogs = 0
			}
			if tc.name == "paged-catalog" {
				wantCatalogs = 2
			}
			require.Equal(t, wantCatalogs, catalogs)
			require.Equal(t, tc.scope, diag.DMsMissingScope)
			require.Equal(t, tc.failure, diag.DMProbeError)
			require.True(t, diag.UserAuthAvailable)
			require.Empty(t, diag.UserAuthError)
			require.Equal(t, tc.name != "excluded", diag.DMsIncluded)
			require.Equal(t, "full", diag.ThreadCoverage)
			require.Empty(t, diag.ThreadCoverageReason)
			data, err := json.Marshal(diag)
			require.NoError(t, err)
			require.NotContains(t, string(data), "probe-private-canary")
			if tc.failure == "" {
				require.NotContains(t, string(data), `"dm_probe_error"`)
			}
		})
	}
}

func TestDoctorStopsOnCallerCancellation(t *testing.T) {
	for _, phase := range []string{"entry", "deadline", "bot-auth", "optional-auth", "catalog", "first-sample"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := error(context.Canceled)
			if phase == "deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer deadlineCancel()
				wantErr = context.DeadlineExceeded
			} else if phase == "entry" {
				cancel()
			}
			var calls []string
			client := primaryOwnerClient(t, config.Tokens{Bot: "fixture-bot", User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				key := r.URL.Path
				if key == "/auth.test" {
					key += ":" + form.Get("token")
				}
				calls = append(calls, key)
				if phase == "bot-auth" && key == "/auth.test:fixture-bot" || phase == "optional-auth" && key == "/auth.test:fixture-user" || phase == "catalog" && key == "/conversations.list" || phase == "first-sample" && key == "/conversations.history" {
					cancel()
				}
				if r.URL.Path == "/conversations.list" {
					return json.RawMessage(`{"ok":true,"channels":[{"id":"D1","is_im":true},{"id":"G1","is_mpim":true}]}`), nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			}).WithDMPolicy(admission.Include)
			_, err := client.Doctor(ctx)
			require.ErrorIs(t, err, wantErr)
			want := []string(nil)
			if phase != "entry" && phase != "deadline" {
				want = append(want, "/auth.test:fixture-bot")
			}
			if phase == "optional-auth" || phase == "catalog" || phase == "first-sample" {
				want = append(want, "/auth.test:fixture-user")
			}
			if phase == "catalog" || phase == "first-sample" {
				want = append(want, "/conversations.list")
			}
			if phase == "first-sample" {
				want = append(want, "/conversations.history")
			}
			require.Equal(t, want, calls)
		})
	}
}
