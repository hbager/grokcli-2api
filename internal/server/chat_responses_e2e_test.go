package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/auth"
	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/server"
)

func TestChatUsesLiveUpstreamRetryCount(t *testing.T) {
	var mu sync.Mutex
	attempts := []string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		attempts = append(attempts, token)
		mu.Unlock()
		if token == "bad" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"sensitive upstream body"}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}],\"usage\":{\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	retries := 0
	h := server.NewMux(server.Options{
		Ready:              func() bool { return true },
		ChatEnabled:        true,
		Candidates:         []pool.Candidate{{ID: "bad", Token: "bad", Enabled: true, RequestCount: 0}, {ID: "ok", Token: "ok", Enabled: true, RequestCount: 1}},
		Config:             config.Config{UpstreamBase: upstream.URL + "/v1", DefaultModel: "grok-4.5"},
		UpstreamRetryCount: func(context.Context) int { return retries },
	})

	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	first := request()
	if first.Code != http.StatusServiceUnavailable || !strings.Contains(first.Body.String(), "all_accounts_failed") {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if strings.Contains(first.Body.String(), "sensitive upstream body") {
		t.Fatalf("first response leaked upstream body: %s", first.Body.String())
	}
	retries = 1
	second := request()
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"content":"ok"`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	mu.Lock()
	gotAttempts := strings.Join(attempts, ",")
	mu.Unlock()
	if gotAttempts != "bad,bad,ok" {
		t.Fatalf("attempts=%s", gotAttempts)
	}
}

func TestAllProtocolsReturnStandardUpstreamErrorAfterExhaustion(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"sensitive upstream body"}`))
	}))
	defer upstream.Close()

	h := server.NewMux(server.Options{
		Ready:              func() bool { return true },
		ChatEnabled:        true,
		MessagesEnabled:    true,
		ResponsesEnabled:   true,
		Candidates:         []pool.Candidate{{ID: "bad", Token: "bad", Enabled: true}},
		Config:             config.Config{UpstreamBase: upstream.URL + "/v1", DefaultModel: "grok-4.5"},
		UpstreamRetryCount: func(context.Context) int { return 3 },
	})

	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "chat", path: "/v1/chat/completions", body: `{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`},
		{name: "messages", path: "/v1/messages", body: `{"model":"grok-4.5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`},
		{name: "responses", path: "/v1/responses", body: `{"model":"grok-4.5","input":[{"role":"user","content":"hi"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			for _, marker := range []string{"upstream_error", "all_accounts_failed"} {
				if !strings.Contains(body, marker) {
					t.Fatalf("missing %q in %s", marker, body)
				}
			}
			if strings.Contains(body, "sensitive upstream body") || strings.Contains(body, "Bearer") {
				t.Fatalf("leaked sensitive upstream data: %s", body)
			}
		})
	}
}

func TestStreamingProtocolsReturnSSEAfterExhaustion(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"scripted"}`))
	}))
	defer upstream.Close()

	h := server.NewMux(server.Options{
		Ready:              func() bool { return true },
		ChatEnabled:        true,
		MessagesEnabled:    true,
		ResponsesEnabled:   true,
		Candidates:         []pool.Candidate{{ID: "bad", Token: "bad", Enabled: true}},
		Config:             config.Config{UpstreamBase: upstream.URL + "/v1", DefaultModel: "grok-4.5"},
		UpstreamRetryCount: func(context.Context) int { return 3 },
	})

	tests := []struct {
		name    string
		path    string
		body    string
		markers []string
	}{
		{name: "chat", path: "/v1/chat/completions", body: `{"model":"grok-4.5","stream":true,"messages":[{"role":"user","content":"hi"}]}`, markers: []string{"data: ", "all_accounts_failed", "data: [DONE]"}},
		{name: "messages", path: "/v1/messages", body: `{"model":"grok-4.5","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, markers: []string{"event: error", "all_accounts_failed"}},
		{name: "responses", path: "/v1/responses", body: `{"model":"grok-4.5","stream":true,"input":[{"role":"user","content":"hi"}]}`, markers: []string{"event: response.failed", "all_accounts_failed", "data: [DONE]"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if contentType := rec.Header().Get("Content-Type"); !strings.Contains(contentType, "text/event-stream") {
				t.Fatalf("content-type=%q body=%s", contentType, rec.Body.String())
			}
			for _, marker := range tt.markers {
				if !strings.Contains(rec.Body.String(), marker) {
					t.Fatalf("missing %q in %s", marker, rec.Body.String())
				}
			}
		})
	}
}

func TestChatAndResponsesE2EAgainstFakeUpstream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		frames := []string{
			`data: {"id":"chatcmpl_x","choices":[{"delta":{"reasoning_content":"plan "}}]}` + "\n\n",
			`data: {"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}` + "\n\n",
			"data: [DONE]\n\n",
		}
		for _, frame := range frames {
			_, _ = w.Write([]byte(frame))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer upstream.Close()

	opts := server.Options{
		Ready:            func() bool { return true },
		ChatEnabled:      true,
		ResponsesEnabled: true,
		APIKeys:          auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "true"}, nil),
		Candidates:       []pool.Candidate{{ID: "acc", Token: "t", Enabled: true}},
		Config:           config.Config{UpstreamBase: upstream.URL + "/v1", DefaultModel: "grok-4.5", SSEKeepalive: 4 * time.Second},
	}
	h := server.NewMux(opts)

	t.Run("chat non-stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("X-Grok2API-Protocol") != "openai_chat" {
			t.Fatalf("headers=%v", rec.Header())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		choices, _ := payload["choices"].([]any)
		if len(choices) == 0 {
			t.Fatalf("payload %#v", payload)
		}
		msg := choices[0].(map[string]any)["message"].(map[string]any)
		if msg["content"] != "hi" {
			t.Fatalf("message %#v", msg)
		}
		if msg["reasoning_content"] != "plan " && msg["reasoning_content"] != "plan" {
			// rawString preserves trailing space if present
			if rc, _ := msg["reasoning_content"].(string); !strings.Contains(rc, "plan") {
				t.Fatalf("reasoning %#v", msg["reasoning_content"])
			}
		}
	})

	t.Run("chat stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"grok-4.5","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		body := rec.Body.String()
		for _, marker := range []string{"data: ", "hi", "data: [DONE]"} {
			if !strings.Contains(body, marker) {
				t.Fatalf("missing %q in %q", marker, body)
			}
		}
	})

	t.Run("responses stream with reasoning", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-4.5","stream":true,"input":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		body := rec.Body.String()
		for _, marker := range []string{"event: response.created", "reasoning_summary_text.delta", "response.output_text.delta", "event: response.completed", "data: [DONE]"} {
			if !strings.Contains(body, marker) {
				t.Fatalf("missing %q in %q", marker, body)
			}
		}
		if rec.Header().Get("X-Grok2API-Protocol") != "openai_responses" {
			t.Fatalf("headers=%v", rec.Header())
		}
	})
}
