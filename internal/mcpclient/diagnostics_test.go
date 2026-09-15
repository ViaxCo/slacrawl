package mcpclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const diagnosticCanary = "SYNTHETIC_PRIVATE_MCP_CONTENT"
const diagnosticNumber = "867530912345678901234567890"

func TestResponseDiagnosticsExcludeServerContent(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		for _, tc := range []struct {
			name, response, want string
			list                 bool
		}{
			{"RPC error", `{"id":1,"error":{"code":-32603,"message":"` + diagnosticCanary + `","data":{"private":"` + diagnosticCanary + `"}}}`, "JSON-RPC error -32603", false},
			{"invalid envelope", `{"id":1,"error":{"code":` + diagnosticNumber + `}}`, "JSON-RPC response", false},
			{"invalid result", `{"id":1,"result":{"isError":` + diagnosticNumber + `}}`, "result: invalid response", false},
			{"tool error", `{"id":1,"result":{"isError":true,"content":[{"type":"text","text":"` + diagnosticCanary + `"}]}}`, "tools/call reported an error", false},
			{"empty tool error", `{"id":1,"result":{"isError":true}}`, "tools/call reported an error", false},
			{"empty tool result", `{"id":1,"result":{"content":[]}}`, "tools/call returned no text content", false},
			{"repeated cursor", `{"id":1,"result":{"tools":[],"nextCursor":"` + diagnosticCanary + `"}}`, "tools/list repeated cursor", true},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var client Session
				if transport == "http" {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						_, err := io.WriteString(w, tc.response)
						require.NoError(t, err)
					}))
					defer server.Close()
					var err error
					client, err = New(Options{Endpoint: server.URL})
					require.NoError(t, err)
				} else {
					var err error
					client, err = NewStdio(ctx, StdioOptions{
						Command: os.Args[0], Args: []string{"-test.run=^TestDiagnosticsStdioHelper$"},
						Env: []string{"MCP_DIAGNOSTIC_RESPONSE=" + tc.response},
					})
					require.NoError(t, err)
				}
				defer func() { require.NoError(t, client.Close()) }()
				var err error
				if tc.list {
					_, err = client.ListTools(ctx)
				} else {
					var text string
					text, err = client.CallToolText(ctx, diagnosticCanary, nil)
					require.Empty(t, text)
				}
				require.ErrorContains(t, err, tc.want)
				assertSafeDiagnostic(t, err)
			})
		}
	}
}

func TestDiagnosticsStdioHelper(t *testing.T) {
	response := os.Getenv("MCP_DIAGNOSTIC_RESPONSE")
	if response == "" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request struct {
			ID json.RawMessage `json:"id"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &request))
		var packet map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(response), &packet))
		packet["id"] = request.ID
		_, err := fmt.Fprintln(os.Stderr, diagnosticCanary)
		require.NoError(t, err)
		require.NoError(t, json.NewEncoder(os.Stdout).Encode(packet))
	}
	require.NoError(t, scanner.Err())
}

func TestHTTPDiagnosticsExcludeBodiesAndRedirects(t *testing.T) {
	for _, notification := range []bool{false, true} {
		for _, tc := range []struct{ name, location, want string }{
			{"HTTP failure", "", "HTTP 503"},
			{"origin redirect", "https://other.example/" + diagnosticCanary, "credential origin"},
			{"invalid redirect", "/%" + diagnosticCanary, "request failed"},
		} {
			t.Run(fmt.Sprintf("%s/notification=%v", tc.name, notification), func(t *testing.T) {
				calls := 0
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls++
					if tc.location != "" {
						w.Header().Set("Location", tc.location)
						w.WriteHeader(http.StatusTemporaryRedirect)
					} else {
						w.WriteHeader(http.StatusServiceUnavailable)
					}
					_, err := io.WriteString(w, diagnosticCanary)
					require.NoError(t, err)
				}))
				defer server.Close()
				client, err := New(Options{Endpoint: server.URL, HTTPClient: server.Client(), RestrictAuthOrigin: true})
				require.NoError(t, err)
				if notification {
					err = client.notify(context.Background(), "notifications/initialized", nil)
				} else {
					_, err = client.ListTools(context.Background())
				}
				require.ErrorContains(t, err, tc.want)
				assertSafeDiagnostic(t, err)
				require.Equal(t, 1, calls)
			})
		}
	}
}

type diagnosticReader struct{ err error }

func (r diagnosticReader) Read([]byte) (int, error) { return 0, r.err }

func TestHTTPDiagnosticsPreserveErrorIdentity(t *testing.T) {
	for _, notification := range []bool{false, true} {
		for _, readBody := range []bool{false, true} {
			for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("caller declined redirect")} {
				t.Run(fmt.Sprintf("notification=%v/body=%v/%v", notification, readBody, cause), func(t *testing.T) {
					failure := errors.Join(errors.New(diagnosticCanary), cause)
					client, err := New(Options{Endpoint: "https://server.example/mcp", HTTPClient: &http.Client{
						Transport: redirectTransport(func(*http.Request) (*http.Response, error) {
							if !readBody {
								return nil, failure
							}
							return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(diagnosticReader{failure})}, nil
						}),
					}})
					require.NoError(t, err)
					if notification {
						err = client.notify(context.Background(), "notifications/initialized", nil)
					} else {
						_, err = client.ListTools(context.Background())
					}
					require.ErrorIs(t, err, cause)
					require.Nil(t, errors.Unwrap(err))
					assertSafeDiagnostic(t, err)
				})
			}
		}
	}
}

func TestToolSuccessContentRemainsUnchanged(t *testing.T) {
	text, err := callToolText(context.Background(), func(_ context.Context, _ string, _ any, out any) error {
		return json.Unmarshal([]byte(`{"content":[{"text":"  `+diagnosticCanary+`  "},{"type":"image","text":"ignored"},{"type":"text","text":"second"}]}`), out)
	}, diagnosticCanary, nil)
	require.NoError(t, err)
	require.Equal(t, "  "+diagnosticCanary+"  \nsecond", text)
}

type diagnosticMarshaler struct{}

func (diagnosticMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New(diagnosticCanary)
}

func TestRequestDiagnosticsExcludeInvalidInputs(t *testing.T) {
	for _, notification := range []bool{false, true} {
		for _, invalidEndpoint := range []bool{false, true} {
			t.Run(fmt.Sprintf("notification=%v/endpoint=%v", notification, invalidEndpoint), func(t *testing.T) {
				endpoint := "https://server.example/mcp"
				var params any = diagnosticMarshaler{}
				if invalidEndpoint {
					endpoint = "https://server.example/%" + diagnosticCanary
					params = nil
				}
				client, err := New(Options{Endpoint: endpoint})
				require.NoError(t, err)
				if notification {
					err = client.notify(context.Background(), "notifications/initialized", params)
				} else {
					err = client.call(context.Background(), "tools/call", params, nil)
				}
				assertSafeDiagnostic(t, err)
			})
		}
	}
	client := &StdioClient{}
	assertSafeDiagnostic(t, client.write(context.Background(), diagnosticMarshaler{}))
}

type diagnosticWriter struct{ err error }

func (w diagnosticWriter) Write([]byte) (int, error) { return 0, w.err }
func (w diagnosticWriter) Close() error              { return nil }

func TestStdioWriteDiagnosticsPreserveErrorIdentity(t *testing.T) {
	cause := errors.Join(errors.New(diagnosticCanary), context.Canceled)
	client := &StdioClient{stdin: diagnosticWriter{cause}}
	err := client.write(context.Background(), map[string]any{"method": "tools/call"})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, errors.Unwrap(err))
	assertSafeDiagnostic(t, err)
}

func assertSafeDiagnostic(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	for _, format := range []string{"%v", "%+v", "%s"} {
		text := fmt.Sprintf(format, err)
		require.NotContains(t, text, diagnosticCanary)
		require.NotContains(t, text, diagnosticNumber)
		require.NotContains(t, strings.ToLower(text), "https://")
	}
}
