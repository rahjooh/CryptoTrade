package okx

import "net/http"

// userAgentTransport wraps an existing RoundTripper and sets a custom
// User-Agent header on all outgoing requests.
type userAgentTransport struct {
	agent string
	base  http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.agent != "" {
		req.Header.Set("User-Agent", t.agent)
	}
	if t.base != nil {
		return t.base.RoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}
