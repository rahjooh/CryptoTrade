package okx

import "net/http"

type userAgentTransport struct {
	agent string
	base  http.RoundTripper
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.agent)
	return t.base.RoundTrip(req)
}
