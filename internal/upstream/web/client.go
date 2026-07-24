package web

import (
	"net/http"
	"strings"

	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

const DefaultBaseURL = "https://grok.com"

// Client talks to Grok Web app-chat API using an SSO cookie.
// Phase 3 scope: chat modes only (fast/auto/expert/heavy). Image/video stay catalog-only.
type Client struct {
	BaseURL   string
	HTTP      *http.Client
	UA        string
	StatsigID string
}

func (c *Client) base() string {
	b := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if b == "" {
		b = DefaultBaseURL
	}
	return b
}

func (c *Client) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return grok.NewHTTPClient(nil)
}
