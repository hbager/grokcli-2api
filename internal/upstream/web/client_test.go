package web

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hm2899/grokcli-2api/internal/pool"
)

func TestModeForModel(t *testing.T) {
	mode, ok := ModeForModel("web/grok-chat-fast")
	if !ok || mode != "fast" {
		t.Fatalf("mode=%q ok=%v", mode, ok)
	}
	if _, ok := ModeForModel("grok-imagine-image"); ok {
		t.Fatal("image should not map")
	}
}

func TestExtractUserPrompt(t *testing.T) {
	p := extractUserPrompt(map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hello"}}})
	if p != "hello" {
		t.Fatalf("prompt=%q", p)
	}
}

func TestWebOpenBridgesText(t *testing.T) {
	frames := []map[string]any{
		{"result": map[string]any{"response": map[string]any{"token": "po", "isThinking": false, "messageTag": "final"}}},
		{"result": map[string]any{"response": map[string]any{"token": "ng", "isThinking": false, "messageTag": "final"}}},
		{"result": map[string]any{"response": map[string]any{"modelResponse": map[string]any{"message": "pong"}}}},
	}
	var body strings.Builder
	for _, f := range frames {
		raw, _ := json.Marshal(f)
		body.Write(raw)
		body.Write([]byte{10})
	}
	var gotCookie, gotMode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotMode, _ = req["modeId"].(string)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body.String())
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client()}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "a", SSO: "eyJweb"}, "grok-chat-fast", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.Contains(gotCookie, "sso=eyJweb") {
		t.Fatalf("cookie=%q", gotCookie)
	}
	if gotMode != "fast" {
		t.Fatalf("mode=%q", gotMode)
	}
	raw, _ := io.ReadAll(resp.Body)
	text := string(raw)
	if !strings.Contains(text, "chat.completion.chunk") || !strings.Contains(text, "po") || !strings.Contains(text, "[DONE]") {
		t.Fatalf("bad bridge: %s", text)
	}
}
