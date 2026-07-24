package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

const DefaultBaseURL = "https://console.x.ai"

// Client talks to Grok Console Responses API using an SSO cookie.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	UA      string
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

func (c *Client) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	// Reuse build client transport defaults (proxy-aware via env / outboundproxy).
	return grok.NewHTTPClient(nil)
}

// Open converts a chat-style body to Responses, POSTs to Console, and bridges
// SSE back to chat.completion.chunk so proxy/chat can stay unchanged.
func (c *Client) Open(ctx context.Context, account pool.ConsoleAccount, model string, body map[string]any, fixedEffort ...string) (*http.Response, error) {
	if strings.TrimSpace(account.SSO) == "" {
		return nil, &grok.UpstreamError{Status: 401, Body: "console requires SSO cookie"}
	}
	ctx = outboundproxy.WithAccountID(ctx, account.ID)
	effort := ""
	if len(fixedEffort) > 0 {
		effort = strings.TrimSpace(fixedEffort[0])
	}
	payload := normalizeConsoleBody(body, model, effort)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers(account.SSO) {
		req.Header.Set(k, v)
	}
	stream := true
	if v, ok := payload["stream"].(bool); ok {
		stream = v
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
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

func (c *Client) headers(sso string) map[string]string {
	ua := strings.TrimSpace(c.UA)
	if ua == "" {
		ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
	}
	sso = strings.TrimSpace(sso)
	if strings.HasPrefix(strings.ToLower(sso), "sso=") {
		sso = strings.TrimSpace(strings.SplitN(sso, "=", 2)[1])
	}
	return map[string]string{
		"Accept":          "*/*",
		"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
		"Authorization":   "Bearer anonymous",
		"Content-Type":    "application/json",
		"Cookie":          "sso=" + sso + "; sso-rw=" + sso,
		"Origin":          "https://console.x.ai",
		"Referer":         "https://console.x.ai/",
		"Sec-Fetch-Dest":  "empty",
		"Sec-Fetch-Mode":  "cors",
		"Sec-Fetch-Site":  "same-origin",
		"User-Agent":      ua,
		"x-cluster":       "https://us-east-1.api.x.ai",
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
