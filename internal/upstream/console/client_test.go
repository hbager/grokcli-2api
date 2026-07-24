package console

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hm2899/grokcli-2api/internal/pool"
)

func TestNormalizeConsoleBodyStripsCacheFields(t *testing.T) {
	body := map[string]any{
		"messages":         []any{map[string]any{"role": "user", "content": "hi"}},
		"prompt_cache_key": "x",
		"tools":            []any{map[string]any{"type": "x_search"}},
	}
	out := normalizeConsoleBody(body, "grok-4.3")
	if out["model"] != "grok-4.3" {
		t.Fatalf("model=%v", out["model"])
	}
	if _, ok := out["prompt_cache_key"]; ok {
		t.Fatal("prompt_cache_key should be stripped")
	}
	if tools, ok := out["tools"].([]any); ok {
		for _, raw := range tools {
			m, _ := raw.(map[string]any)
			if m["type"] == "x_search" {
				t.Fatal("x_search should be removed")
			}
		}
	}
	if out["store"] != false {
		t.Fatalf("store=%v", out["store"])
	}
}

func TestConsoleOpenUsesSSOCookie(t *testing.T) {
	var gotCookie string
	var gotAuth string
	var gotModel string
	payload := "event: response.completed\ndata: " + `{"type":"response.completed","response":{"id":"r1","model":"grok-4.3","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		gotAuth = r.Header.Get("Authorization")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotModel, _ = body["model"].(string)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, payload)
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client()}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "a", SSO: "eyJtest"}, "grok-4.3", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.Contains(gotCookie, "sso=eyJtest") {
		t.Fatalf("cookie=%q", gotCookie)
	}
	if gotAuth != "Bearer anonymous" {
		t.Fatalf("auth=%q", gotAuth)
	}
	if gotModel != "grok-4.3" {
		t.Fatalf("model=%q", gotModel)
	}
}

func TestNormalizeConsoleBodyMultiAgentXHigh(t *testing.T) {
	body := map[string]any{
		"messages":         []any{map[string]any{"role": "user", "content": "hi"}},
		"max_tokens":       99,
		"reasoning_effort": "low",
	}
	out := normalizeConsoleBody(body, "grok-4.20-multi-agent", "xhigh")
	if out["model"] != "grok-4.20-multi-agent" {
		t.Fatalf("model=%v", out["model"])
	}
	if _, ok := out["max_output_tokens"]; ok {
		t.Fatal("max_output_tokens should be stripped for multi-agent")
	}
	r, _ := out["reasoning"].(map[string]any)
	if r == nil || r["effort"] != "xhigh" {
		t.Fatalf("reasoning=%#v want effort=xhigh", out["reasoning"])
	}
}
