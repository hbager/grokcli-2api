package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/accounts"
	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/browserhttp"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

const DefaultBaseURL = "https://console.x.ai"

// Client talks to Grok Console Responses API using an SSO cookie.
// BrowserTLS (default on) uses Chrome JA3/H2 impersonation — plain Go
// net/http is fingerprint-blocked by Cloudflare on console.x.ai.
type Client struct {
	BaseURL string
	// HTTP optional stdlib client (tests / explicit override). When set and
	// BrowserTLS is false, used as-is. When BrowserTLS is true and HTTP is
	// set, BrowserTLS still wins unless DisableBrowserTLS is true.
	HTTP *http.Client
	// Proxy optional dynamic outbound proxy (same signature as http.Transport.Proxy).
	Proxy func(*http.Request) (*url.URL, error)
	// ProxyURL optional static proxy URL.
	ProxyURL string
	// DisableBrowserTLS forces plain HTTP (only for unit tests with httptest).
	DisableBrowserTLS bool
	UA                string

	transportMu sync.Mutex
	browser     *browserhttp.Client
	plain       *http.Client
	dpop        dpopSessionManager
}

func (c *Client) base() string {
	b := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if b == "" {
		b = DefaultBaseURL
	}
	return b
}

func (c *Client) endpoint() string {
	b := c.base()
	if strings.HasSuffix(b, "/v1") {
		return b + "/responses"
	}
	return b + "/v1/responses"
}

func (c *Client) dpopTokenEndpoint() string {
	b := c.base()
	if strings.HasSuffix(b, "/v1") {
		return b + "/dpop/token"
	}
	return b + "/v1/dpop/token"
}

// doer is the minimal interface used by Open (stdlib or browser TLS).
type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

func (c *Client) proxyResolver() func(*http.Request) (*url.URL, error) {
	if c == nil {
		return nil
	}
	if c.Proxy != nil {
		return c.Proxy
	}
	if c.HTTP != nil {
		if tr, ok := c.HTTP.Transport.(*http.Transport); ok {
			return tr.Proxy
		}
	}
	return nil
}

func (c *Client) transport() doer {
	if c == nil {
		return grok.NewHTTPClient(nil)
	}
	proxyFn := c.proxyResolver()
	// Unit tests: httptest injects HTTP + DisableBrowserTLS. A cloned client is
	// needed when its transport has a proxy so an explicit direct binding also
	// bypasses that proxy instead of falling back to the original resolver.
	if c.DisableBrowserTLS {
		if c.HTTP != nil && proxyFn == nil {
			return c.HTTP
		}
		c.transportMu.Lock()
		defer c.transportMu.Unlock()
		if c.plain == nil {
			if c.HTTP != nil {
				clone := *c.HTTP
				if tr, ok := c.HTTP.Transport.(*http.Transport); ok {
					transport := tr.Clone()
					transport.Proxy = pinnedProxyResolver(proxyFn)
					clone.Transport = transport
				}
				c.plain = &clone
			} else {
				c.plain = grok.NewHTTPClient(pinnedProxyResolver(proxyFn))
			}
		}
		return c.plain
	}
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	// Production: Chrome TLS/HTTP2 fingerprint (plain Go net/http is CF-blocked).
	if c.browser == nil {
		c.browser = &browserhttp.Client{
			Profile: "chrome_131",
			Timeout: 120 * time.Second,
			Proxy:   pinnedProxyResolver(proxyFn),
		}
	}
	return c.browser
}

func pinnedProxyResolver(fallback func(*http.Request) (*url.URL, error)) func(*http.Request) (*url.URL, error) {
	return func(req *http.Request) (*url.URL, error) {
		if req != nil {
			if proxyURL, ok := outboundproxy.PinnedProxy(req.Context()); ok {
				return proxyURL, nil
			}
		}
		if fallback == nil {
			return nil, nil
		}
		return fallback(req)
	}
}

func (c *Client) pinEgress(ctx context.Context) (context.Context, string, error) {
	probe, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base(), nil)
	if err != nil {
		return nil, "", err
	}
	var proxyURL *url.URL
	if resolver := c.proxyResolver(); resolver != nil {
		proxyURL, err = resolver(probe)
		if err != nil {
			return nil, "", err
		}
	}
	if proxyURL == nil {
		if raw := strings.TrimSpace(c.ProxyURL); raw != "" {
			proxyURL, err = url.Parse(raw)
			if err != nil {
				return nil, "", fmt.Errorf("parse Console proxy URL: %w", err)
			}
		}
	}
	identity := "direct"
	if proxyURL != nil {
		identity = canonicalProxyIdentity(proxyURL)
	}
	return outboundproxy.WithPinnedProxy(ctx, proxyURL), identity, nil
}

func canonicalProxyIdentity(proxyURL *url.URL) string {
	clone := *proxyURL
	clone.Scheme = strings.ToLower(clone.Scheme)
	clone.Host = strings.ToLower(clone.Host)
	clone.Fragment = ""
	clone.RawFragment = ""
	return clone.String()
}

// Open converts a chat-style body to Responses, POSTs to Console, and bridges
// SSE back to chat.completion.chunk so proxy/chat can stay unchanged.
func (c *Client) Open(ctx context.Context, account pool.ConsoleAccount, model string, body map[string]any, fixedEffort ...string) (*http.Response, error) {
	if strings.TrimSpace(account.SSO) == "" {
		return nil, &grok.UpstreamError{Status: 401, Body: "console requires SSO cookie"}
	}
	ctx = outboundproxy.WithAccountID(ctx, account.ID)
	ctx, egressIdentity, err := c.pinEgress(ctx)
	if err != nil {
		return nil, err
	}
	effort := ""
	if len(fixedEffort) > 0 {
		effort = strings.TrimSpace(fixedEffort[0])
	}
	payload := normalizeConsoleBody(body, model, effort)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	stream := true
	if v, ok := payload["stream"].(bool); ok {
		stream = v
	}
	transport := c.transport()
	cacheKey := c.dpopCacheKey(account, egressIdentity)
	var resp *http.Response
	for attempt := 0; attempt < 2; attempt++ {
		session, sessionErr := c.dpop.get(ctx, cacheKey, func(loadCtx context.Context) (dpopSession, error) {
			return c.mintDPoPSession(loadCtx, transport, account)
		})
		if sessionErr != nil {
			return nil, sessionErr
		}
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(encoded))
		if requestErr != nil {
			return nil, requestErr
		}
		for k, v := range c.headers(account.SSO, account.Cookies) {
			req.Header.Set(k, v)
		}
		req.Header.Set("x-cluster", "https://us-east-1.api.x.ai")
		if stream {
			req.Header.Set("Accept", "text/event-stream")
		}
		if requestErr = applyDPoPAuthorization(req, session); requestErr != nil {
			return nil, requestErr
		}
		resp, requestErr = transport.Do(req)
		if requestErr != nil {
			return nil, requestErr
		}
		if resp.StatusCode != http.StatusUnauthorized || attempt == 1 {
			break
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		c.dpop.invalidate(cacheKey, session.accessToken)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return nil, &grok.UpstreamError{
			Status: resp.StatusCode, Body: string(errBody),
			RetryAfter: resp.Header.Get("Retry-After"),
		}
	}
	// Bridge Responses SSE -> chat chunks (exported helper on grok package).
	resp.Body = grok.ResponsesToChatStream(resp.Body)
	if resp.Header != nil {
		resp.Header.Set("Content-Type", "text/event-stream")
		resp.Header.Set("X-Grok2API-Upstream-Protocol", "console-responses")
		resp.Header.Set("X-Grok2API-Provider", "console")
	}
	return resp, nil
}

func (c *Client) headers(sso string, cfCookies map[string]string) map[string]string {
	ua := strings.TrimSpace(c.UA)
	if ua == "" {
		// Match registration / curl_cffi chrome131 surface.
		ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	}
	return map[string]string{
		"Accept":             "*/*",
		"Accept-Language":    "en-US,en;q=0.9",
		"Content-Type":       "application/json",
		"Cookie":             accounts.BuildSSOCookieHeader(sso, cfCookies),
		"Origin":             "https://console.x.ai",
		"Referer":            "https://console.x.ai/",
		"Sec-Ch-Ua":          `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
		"Sec-Ch-Ua-Mobile":   "?0",
		"Sec-Ch-Ua-Platform": `"Windows"`,
		"Sec-Fetch-Dest":     "empty",
		"Sec-Fetch-Mode":     "cors",
		"Sec-Fetch-Site":     "same-origin",
		"User-Agent":         ua,
	}
}

// normalizeConsoleBody builds a stateless Console Responses body from chat/raw maps.
func normalizeConsoleBody(body map[string]any, model string, fixedEffort ...string) map[string]any {
	// Reuse build conversion for messages/tools, then strip CPA-only fields.
	out := grok.ChatToResponsesPayload(body, model)
	out["model"] = model
	out["store"] = false
	// Console is stateless; drop sticky / cache fields that confuse it.
	for _, key := range []string{
		"metadata", "previous_response_id", "service_tier", "prompt_cache_key",
		"background", "conversation", "user",
	} {
		delete(out, key)
	}
	// Drop CPA-only built-in search tools when present as bare x_search.
	if tools, ok := out["tools"].([]any); ok {
		filtered := make([]any, 0, len(tools))
		for _, raw := range tools {
			m, ok := raw.(map[string]any)
			if !ok {
				filtered = append(filtered, raw)
				continue
			}
			t, _ := m["type"].(string)
			if strings.EqualFold(t, "x_search") {
				continue
			}
			filtered = append(filtered, raw)
		}
		if len(filtered) == 0 {
			delete(out, "tools")
		} else {
			out["tools"] = filtered
		}
	}
	// Multi-agent docs: max_tokens not supported.
	if isMultiAgentModel(model) {
		delete(out, "max_output_tokens")
		delete(out, "max_tokens")
	}
	effort := ""
	if len(fixedEffort) > 0 {
		effort = strings.TrimSpace(fixedEffort[0])
	}
	if effort != "" {
		out["reasoning"] = map[string]any{"effort": effort, "summary": "auto"}
	}
	// Ensure stream default true for SSE bridge path.
	if _, ok := out["stream"]; !ok {
		out["stream"] = true
	}
	return out
}

func isMultiAgentModel(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	return strings.Contains(m, "multi-agent")
}

// Idle keep-alive for long console streams is handled by transport layer.
var _ = time.Second

// OpenWithFailover mirrors proxy.OpenWithFailover for console accounts.
func OpenWithFailover(ctx context.Context, client *Client, accounts []pool.ConsoleAccount, model string, body map[string]any) (pool.ConsoleAccount, io.ReadCloser, error) {
	if client == nil {
		client = &Client{}
	}
	var last error
	for _, account := range accounts {
		resp, err := client.Open(ctx, account, model, body)
		if err == nil {
			return account, resp.Body, nil
		}
		last = err
		if !grok.Retryable(err) {
			break
		}
	}
	if last == nil {
		last = fmt.Errorf("no eligible console accounts")
	}
	return pool.ConsoleAccount{}, nil, last
}
