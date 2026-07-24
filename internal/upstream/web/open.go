package web

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

// Open posts a new temporary web conversation and bridges JSON stream to chat SSE.
func (c *Client) Open(ctx context.Context, account pool.ConsoleAccount, model string, body map[string]any) (*http.Response, error) {
	if strings.TrimSpace(account.SSO) == "" {
		return nil, &grok.UpstreamError{Status: 401, Body: "web requires SSO cookie"}
	}
	mode, ok := ModeForModel(model)
	if !ok {
		return nil, &grok.UpstreamError{Status: 400, Body: "web model not supported for chat: " + strings.TrimSpace(model)}
	}
	ctx = outboundproxy.WithAccountID(ctx, account.ID)
	prompt := extractUserPrompt(body)
	if strings.TrimSpace(prompt) == "" {
		return nil, &grok.UpstreamError{Status: 400, Body: "web chat requires non-empty user message"}
	}
	payload := buildWebChatPayload(prompt, mode)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	endpoint := c.base() + "/rest/app-chat/conversations/new"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers(account.SSO) {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return nil, &grok.UpstreamError{Status: resp.StatusCode, Body: string(errBody), RetryAfter: resp.Header.Get("Retry-After")}
	}
	resp.Body = WebToChatStream(resp.Body, model)
	if resp.Header != nil {
		resp.Header.Set("Content-Type", "text/event-stream")
		resp.Header.Set("X-Grok2API-Upstream-Protocol", "web-app-chat")
		resp.Header.Set("X-Grok2API-Provider", "web")
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
	base := c.base()
	h := map[string]string{
		"Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Content-Type": "application/json",
		"Cookie": "sso=" + sso + "; sso-rw=" + sso, "Origin": base, "Referer": base + "/",
		"Cache-Control": "no-cache", "Pragma": "no-cache", "Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin", "User-Agent": ua,
		"x-xai-request-id": newRequestUUID(),
	}
	if id := strings.TrimSpace(c.StatsigID); id != "" {
		h["x-statsig-id"] = id
	}
	return h
}
