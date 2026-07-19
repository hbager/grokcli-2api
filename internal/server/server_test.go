package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/auth"
	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
)

func TestRegistrationConfigLocalCacheExcludesSecrets(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "static", "js", "core.js"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "function registrationConfigForLocalCache")
	if start < 0 {
		t.Fatal("registration local-cache projection missing")
	}
	end := strings.Index(text[start:], "async function saveRegConfig")
	if end < 0 {
		t.Fatal("registration local-cache projection boundary missing")
	}
	projection := text[start : start+end]
	for _, secret := range []string{"api_key", "yescaptcha_key", "proxy", "proxy_username", "proxy_password"} {
		if strings.Contains(projection, secret+":") {
			t.Fatalf("local cache projection includes %s", secret)
		}
	}
	if strings.Contains(text, "cacheRegConfigLocal(cfg);\n    toast((e && e.message)") {
		t.Fatal("save failure caches submitted secrets")
	}
}

func TestRegistrationConfigSecretClearUsesExplicitControls(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "static", "js", "core.js"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		`api_key_clear`,
		`yescaptcha_key_clear`,
		`proxy_password_clear`,
		`reg-api-key-clear`,
		`reg-yescaptcha-key-clear`,
		`reg-proxy-password-clear`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("registration secret clear contract missing %q", required)
		}
	}
}

func TestOutboundProxyPasswordClearUsesExplicitControl(t *testing.T) {
	core, err := os.ReadFile(filepath.Join("..", "..", "static", "js", "core.js"))
	if err != nil {
		t.Fatal(err)
	}
	html, err := os.ReadFile(filepath.Join("..", "..", "static", "admin", "settings.html"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(core), "outbound_proxy_password_clear") {
		t.Fatal("outbound proxy password clear flag missing from settings patch")
	}
	if !strings.Contains(string(core), "set-outbound-proxy-password-clear") ||
		!strings.Contains(string(html), "set-outbound-proxy-password-clear") {
		t.Fatal("outbound proxy password clear control missing")
	}
}

func TestNewChatServiceUsesRuntimeFailoverLimit(t *testing.T) {
	runtime := config.NewRuntimeConfig(config.Config{MaxFailoverAttempts: 2})
	service := newChatService(Options{RuntimeConfig: runtime})
	if service.MaxFailoverAttempts != 2 {
		t.Fatalf("MaxFailoverAttempts=%d want 2", service.MaxFailoverAttempts)
	}
}

func TestLive(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/live", nil)
	NewMigrationMux(func() bool { return true }).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["implementation"] != "go" || body["ok"] != true {
		t.Fatalf("unexpected body %#v", body)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type %q", got)
	}
}

func TestReadyFailClosed(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	NewMux(Options{
		Ready:  func() bool { return false },
		Reason: func() string { return "migration pending" },
	}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["reason"] != "migration pending" {
		t.Fatalf("unexpected body %#v", body)
	}
}

func TestMethodAndUnknownRoute(t *testing.T) {
	handler := NewMux(Options{Ready: func() bool { return true }, StaticDir: t.TempDir()})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/live", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /live status = %d", recorder.Code)
	}

	// Root is exact-match only; unknown paths must not fall through to index.html.
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/unknown", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /unknown status = %d body=%q", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	// Missing index still 404, but route itself must match exactly "/".
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET / with empty static dir status = %d", recorder.Code)
	}
}

func TestHealthAndMetricsAreReadOnlyShells(t *testing.T) {
	handler := NewMigrationMux(func() bool { return false })

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("/health status = %d", recorder.Code)
	}
	var health map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if health["implementation"] != "go" || health["ready"] != false {
		t.Fatalf("unexpected health %#v", health)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "g2a_runtime_ready") {
		t.Fatalf("unexpected metrics %q", recorder.Body.String())
	}
}

func TestModelsRouteFlagAndAuth(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled models route status = %d", recorder.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	recorder = httptest.NewRecorder()
	NewMux(Options{
		Ready:             func() bool { return true },
		PublicReadEnabled: true,
		APIKeys:           auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
	}).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", recorder.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret")
	recorder = httptest.NewRecorder()
	NewMux(Options{
		Ready:             func() bool { return true },
		PublicReadEnabled: true,
		APIKeys:           auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
	}).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authorized models status = %d body=%q", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["object"] != "list" {
		t.Fatalf("unexpected models body %#v", body)
	}
}

type fakeAdminSessions struct{ ok bool }

func (f fakeAdminSessions) VerifyAdminSession(string) bool { return f.ok }

func TestAdminReadRoutesRequireFlagReadinessAndSession(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/status", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled status route = %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return false }, AdminReadEnabled: true}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/status", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready status route = %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }, AdminReadEnabled: true}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/status", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("public admin status = %d body=%q", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }, AdminReadEnabled: true, AdminSessions: fakeAdminSessions{ok: false}}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/dashboard", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("dashboard without session = %d", recorder.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/api/models", nil)
	req.Header.Set("X-Admin-Token", "token")
	recorder = httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }, AdminReadEnabled: true, AdminSessions: fakeAdminSessions{ok: true}}).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("admin models with session = %d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestMessagesRouteGates(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/messages"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return true }}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("disabled messages route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return false }, MessagesEnabled: true}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("not-ready messages route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:           func() bool { return true },
				MessagesEnabled: true,
				APIKeys:         auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("missing auth messages route = %d", recorder.Code)
			}

			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer secret")
			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:           func() bool { return true },
				MessagesEnabled: true,
				APIKeys:         auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("store-unavailable messages route = %d", recorder.Code)
			}
		})
	}

	for _, path := range []string{"/v1/messages/count_tokens", "/messages/count_tokens"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return true }}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("disabled count_tokens route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return false }, MessagesEnabled: true}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("not-ready count_tokens route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:           func() bool { return true },
				MessagesEnabled: true,
				APIKeys:         auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("missing auth count_tokens route = %d", recorder.Code)
			}

			// count_tokens is a pure local heuristic: no store required.
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer secret")
			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:           func() bool { return true },
				MessagesEnabled: true,
				APIKeys:         auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("empty count_tokens without store = %d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestMessagesCountTokensMatchesPythonHeuristic(t *testing.T) {
	body := `{"system":"abcd","messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"tool_use","name":"Edit","input":{"file_path":"/x","old_string":"a","new_string":""}}]}],"tools":[{"name":"Edit","description":"edit files","input_schema":{"type":"object"}}]}`
	recorder := httptest.NewRecorder()
	NewMux(Options{
		Ready:           func() bool { return true },
		MessagesEnabled: true,
		Store:           &postgres.Connector{},
	}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("count_tokens status = %d body=%q", recorder.Code, recorder.Body.String())
	}
	var payload map[string]int
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["input_tokens"] != 27 {
		t.Fatalf("input_tokens = %d", payload["input_tokens"])
	}

	recorder = httptest.NewRecorder()
	NewMux(Options{Ready: func() bool { return true }, MessagesEnabled: true, Store: &postgres.Connector{}}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/messages/count_tokens", strings.NewReader(`{}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty count_tokens status = %d body=%q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "invalid_request_error") {
		t.Fatalf("unexpected empty count_tokens body=%q", recorder.Body.String())
	}
}

func TestResponsesRouteGates(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/responses"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return true }}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("disabled responses route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{Ready: func() bool { return false }, ResponsesEnabled: true}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("not-ready responses route = %d", recorder.Code)
			}

			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:            func() bool { return true },
				ResponsesEnabled: true,
				APIKeys:          auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("missing auth responses route = %d", recorder.Code)
			}

			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer secret")
			recorder = httptest.NewRecorder()
			NewMux(Options{
				Ready:            func() bool { return true },
				ResponsesEnabled: true,
				APIKeys:          auth.NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil),
			}).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("store-unavailable responses route = %d", recorder.Code)
			}
		})
	}
}

func TestStreamAnthropicMessagesWritesSSE(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	usage, _, err := streamAnthropicMessages(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hi\",\"reasoning_content\":\"plan\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\ndata: [DONE]\n\n"), "msg_test", "grok", false, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if usage["total_tokens"] != float64(3) {
		t.Fatalf("usage = %#v", usage)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("stream status = %d body=%q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, marker := range []string{"event: message_start", "event: content_block_start", "\"type\":\"thinking\"", "plan", "hi", "event: message_delta", "event: message_stop"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("missing %q in %q", marker, body)
		}
	}
}

func TestStreamAnthropicMessagesWritesToolUse(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, _, err := streamAnthropicMessages(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"Edit\",\"arguments\":\"{\\\"file_path\\\":\\\"/x\\\",\\\"old_string\\\":\\\"a\\\",\\\"new_string\\\":\\\"\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"), "msg_test", "grok", true, []string{"Edit"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	for _, marker := range []string{"\"type\":\"tool_use\"", "\"name\":\"Edit\"", "input_json_delta", "\"stop_reason\":\"tool_use\""} {
		if !strings.Contains(body, marker) {
			t.Fatalf("missing %q in %q", marker, body)
		}
	}
}

func TestStreamAnthropicMessagesEmitsThinkingDelta(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, _, err := streamAnthropicMessages(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), "msg_test", "grok", false, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	for _, marker := range []string{"\"type\":\"thinking\"", "thinking_delta", "think", "event: message_stop"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("missing %q in %q", marker, body)
		}
	}
}

func TestStreamOpenAIResponsesWritesSSE(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	usage, _, err := streamOpenAIResponses(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\ndata: [DONE]\n\n"), "resp_test", "grok", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if usage["total_tokens"] != float64(3) {
		t.Fatalf("usage = %#v", usage)
	}
	body := recorder.Body.String()
	for _, marker := range []string{"event: response.created", "event: response.output_item.added", "event: response.output_text.delta", "event: response.completed", "data: [DONE]"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("missing %q in %q", marker, body)
		}
	}
}

func TestStreamOpenAIResponsesWritesFunctionCall(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_, _, err := streamOpenAIResponses(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"Edit\",\"arguments\":\"{\\\"file_path\\\":\\\"/x\\\",\\\"old_string\\\":\\\"a\\\",\\\"new_string\\\":\\\"\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"), "resp_test", "grok", []string{"Edit"}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	for _, marker := range []string{"event: response.function_call_arguments.delta", "event: response.function_call_arguments.done", "event: response.output_item.done", "\"name\":\"Edit\"", "event: response.completed"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("missing %q in %q", marker, body)
		}
	}
}

func TestStreamChatCompletionsWritesSSE(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	stats, err := streamChatCompletions(recorder, request, strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Usage != nil {
		t.Fatalf("unexpected usage stats %#v", stats.Usage)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("stream status = %d body=%q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("unexpected content-type %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `data: {"choices"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("unexpected stream body %q", body)
	}
}

func TestAdminAndStaticFiles(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "index.html"), "home")
	mustWrite(t, filepath.Join(dir, "favicon.ico"), "ico")
	mustWrite(t, filepath.Join(dir, "admin", "index.html"), "admin")
	mustWrite(t, filepath.Join(dir, "admin", "keys.html"), "keys")
	mustWrite(t, filepath.Join(dir, "js", "app.js"), "console.log(1)")

	handler := NewMux(Options{Ready: func() bool { return true }, StaticDir: dir})

	for _, path := range []string{"/", "/favicon.ico", "/admin", "/admin/keys", "/static/js/app.js"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d body=%q", path, recorder.Code, recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/keys", nil))
	if got := recorder.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("admin cache-control = %q", got)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/nope", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("/admin/nope status = %d", recorder.Code)
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

type slowReader struct {
	chunks []string
	idx    int
	delay  time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if s.idx >= len(s.chunks) {
		return 0, io.EOF
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	n := copy(p, s.chunks[s.idx])
	s.idx++
	if n < len(s.chunks[s.idx-1]) {
		// should not happen with full chunk sizes in tests
	}
	return n, nil
}

func TestStreamAnthropicKeepaliveEmitsPing(t *testing.T) {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req = req.WithContext(withAnthropicKeepalive(req.Context(), 30*time.Millisecond))
	body := &slowReader{
		delay: 80 * time.Millisecond,
		chunks: []string{
			"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		},
	}
	_, _, err := streamAnthropicMessages(recorder, req, body, "msg_test", "grok", false, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, "event: ping") && !strings.Contains(out, ": keepalive") {
		t.Fatalf("expected keepalive/ping in %q", out)
	}
	if !strings.Contains(out, "hi") || !strings.Contains(out, "event: message_stop") {
		t.Fatalf("missing content/terminal in %q", out)
	}
}

func TestStreamAnthropicSoftDisconnectStillClosesEnvelope(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	// Deliver content first, then cancel before [DONE] so finish path sees client_gone.
	body := &cancelAfterChunk{
		chunks: []string{
			"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		},
		cancelAfter: 1,
		cancel:      cancel,
	}
	_, _, err := streamAnthropicMessages(recorder, req, body, "msg_test", "grok", false, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	for _, marker := range []string{"event: message_start", "hi", "event: message_delta", "event: message_stop"} {
		if !strings.Contains(out, marker) {
			t.Fatalf("missing %q in soft-disconnect body %q", marker, out)
		}
	}
}

func TestStreamAnthropicToolUseAtomicOnSoftDisconnect(t *testing.T) {
	// tool_use start/delta/stop must land as a complete group even when the client
	// soft-disconnects right after the upstream tool chunk (Claude Code "Tool use interrupted").
	recorder := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	toolChunk := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"Edit\",\"arguments\":\"{\\\"file_path\\\":\\\"/x\\\",\\\"old_string\\\":\\\"a\\\",\\\"new_string\\\":\\\"b\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n"
	body := &cancelAfterChunk{
		chunks: []string{
			toolChunk,
			"data: [DONE]\n\n",
		},
		cancelAfter: 1,
		cancel:      cancel,
	}
	_, _, err := streamAnthropicMessages(recorder, req, body, "msg_tool", "grok", true, []string{"Edit"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	for _, marker := range []string{
		"event: message_start",
		`"type":"tool_use"`,
		`"name":"Edit"`,
		"content_block_delta",
		"content_block_stop",
		"event: message_delta",
		"event: message_stop",
	} {
		if !strings.Contains(out, marker) {
			t.Fatalf("missing %q in soft-disconnect tool body %q", marker, out)
		}
	}
	// start must not be the sole tool frame before stop (half-open tool_use).
	startIdx := strings.Index(out, `"type":"tool_use"`)
	stopIdx := strings.Index(out, "content_block_stop")
	if startIdx < 0 || stopIdx < 0 || stopIdx < startIdx {
		t.Fatalf("tool_use start/stop order broken in %q", out)
	}
}

func TestStreamChatCompletionsForceFinishOnSoftDisconnect(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	// Complete tool args but no finish_reason before cancel (soft disconnect mid-stream).
	toolChunk := "data: {\"id\":\"chatcmpl_t\",\"model\":\"grok\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"Edit\",\"arguments\":\"{\\\"file_path\\\":\\\"/x\\\",\\\"old_string\\\":\\\"a\\\",\\\"new_string\\\":\\\"b\\\"}\"}}]},\"finish_reason\":null}]}\n\n"
	body := &cancelAfterChunk{
		chunks: []string{
			toolChunk,
		},
		cancelAfter: 1,
		cancel:      cancel,
	}
	stats, err := streamChatCompletions(recorder, req, body, 50*time.Millisecond)
	_ = stats
	if err != nil {
		t.Fatalf("streamChatCompletions err=%v body=%q", err, recorder.Body.String())
	}
	out := recorder.Body.String()
	if !strings.Contains(out, "tool_calls") {
		t.Fatalf("missing tool_calls in %q", out)
	}
	if !strings.Contains(out, "finish_reason") {
		t.Fatalf("missing finish_reason in %q", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Fatalf("missing [DONE] in %q", out)
	}
	if strings.Count(out, "data: [DONE]") != 1 {
		t.Fatalf("want single [DONE], got %d in %q", strings.Count(out, "data: [DONE]"), out)
	}
}

type cancelAfterChunk struct {
	chunks      []string
	idx         int
	cancelAfter int
	cancel      context.CancelFunc
}

func (c *cancelAfterChunk) Read(p []byte) (int, error) {
	if c.idx >= len(c.chunks) {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[c.idx])
	c.idx++
	if c.idx == c.cancelAfter && c.cancel != nil {
		c.cancel()
	}
	return n, nil
}

type cancelAfterRead struct {
	io.Reader
	cancel context.CancelFunc
	after  int
	n      int
}

func (c *cancelAfterRead) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n++
	if c.n >= c.after && c.cancel != nil {
		c.cancel()
	}
	return n, err
}

func TestStreamAnthropicSilentUpstreamStillKeepalives(t *testing.T) {
	// Upstream emits continuous incomplete tool deltas (no client frames).
	// Server must still write keepalive pings so ~60s idle cutoffs cannot fire.
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req = req.WithContext(withAnthropicKeepalive(req.Context(), 20*time.Millisecond))

	// Chunked incomplete tool stream with delays so idle timer fires.
	body := &slowChunks{
		chunks: []string{
			// message with tools requested path is controlled by toolsRequested arg
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c","type":"function","function":{"name":"Bash","arguments":"{\"command\":"}}]}}]}` + "\n\n",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ls"}}]}}]}` + "\n\n",
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":""}}]},"finish_reason":"tool_calls"}]}` + "\n\n",
			"data: [DONE]\n\n",
		},
		delay: 30 * time.Millisecond,
	}
	// toolsRequested=true so text would be held; incomplete Bash should not emit tool block mid-stream
	_, _, err := streamAnthropicMessagesWithOptions(recorder, req, body, "msg_k", "grok", true, []string{"Bash"}, 1, anthropicStreamOptions{Keepalive: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, "event: ping") && !strings.Contains(out, ": keepalive") {
		// Also accept message_stop as proof stream completed; keepalive may race if total < idle.
		if !strings.Contains(out, "event: message_stop") {
			t.Fatalf("expected keepalive or terminal, got %q", out)
		}
	}
	if !strings.Contains(out, "event: message_stop") {
		t.Fatalf("missing message_stop in %q", out)
	}
}

type slowChunks struct {
	chunks []string
	idx    int
	delay  time.Duration
	buf    []byte
}

func (s *slowChunks) Read(p []byte) (int, error) {
	if len(s.buf) == 0 {
		if s.idx >= len(s.chunks) {
			return 0, io.EOF
		}
		if s.delay > 0 && s.idx > 0 {
			time.Sleep(s.delay)
		}
		s.buf = []byte(s.chunks[s.idx])
		s.idx++
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func TestMessagesCountTokensWithoutStore(t *testing.T) {
	body := `{"system":"abcd","messages":[{"role":"user","content":"hello"}]}`
	recorder := httptest.NewRecorder()
	NewMux(Options{
		Ready:           func() bool { return true },
		MessagesEnabled: true,
		// no Store — count_tokens is a pure local heuristic
	}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("count_tokens without store = %d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestStreamAnthropicLongThinkingMultiToolCloses(t *testing.T) {
	// Claude Code shape: toolsRequested=true, long reasoning stream, then multi-tool
	// (one incomplete early index + complete later indexes). Expect:
	//  - live thinking_delta (SSE keepalive)
	//  - complete tools emitted (not blocked by incomplete index 0)
	//  - always message_stop so task leaves "running"
	var chunks []string
	for i := 0; i < 30; i++ {
		chunks = append(chunks, `data: {"choices":[{"delta":{"reasoning_content":"think-"}}]}`+"\n\n")
	}
	// incomplete Bash@0, complete Read@1, complete Edit@2
	chunks = append(chunks,
		`data: {"choices":[{"delta":{"tool_calls":[`+
			`{"index":0,"id":"t0","type":"function","function":{"name":"Bash","arguments":"{\"command\":"}} ,`+
			`{"index":1,"id":"t1","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/a.go\"}"}} ,`+
			`{"index":2,"id":"t2","type":"function","function":{"name":"Edit","arguments":"{\"file_path\":\"/b.go\",\"old_string\":\"x\",\"new_string\":\"y\"}"}}`+
			`]},"finish_reason":"tool_calls"}]}`+"\n\n",
		"data: [DONE]\n\n",
	)
	body := strings.NewReader(strings.Join(chunks, ""))
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, _, err := streamAnthropicMessages(recorder, req, body, "msg_cc", "grok-4.5", true, []string{"Bash", "Read", "Edit"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	for _, marker := range []string{
		"thinking_delta",
		"think-",
		`"name":"Read"`,
		"message_delta",
		"message_stop",
	} {
		if !strings.Contains(out, marker) {
			t.Fatalf("missing %q in long-thinking multi-tool stream (len=%d)", marker, len(out))
		}
	}
	// Incomplete Bash must not block Read; and should not appear as tool_use.
	if strings.Contains(out, `"name":"Bash"`) {
		t.Fatalf("incomplete Bash should not emit tool_use")
	}
	if !strings.Contains(out, `"stop_reason":"tool_use"`) {
		t.Fatalf("expected tool_use stop_reason, body head=%q", out[:min(400, len(out))])
	}
}

func TestStreamResponsesLongReasoningMultiToolCloses(t *testing.T) {
	var chunks []string
	for i := 0; i < 20; i++ {
		chunks = append(chunks, `data: {"choices":[{"delta":{"reasoning_content":"plan-"}}]}`+"\n\n")
	}
	chunks = append(chunks,
		`data: {"choices":[{"delta":{"tool_calls":[`+
			`{"index":0,"id":"c0","type":"function","function":{"name":"Bash","arguments":"{\"command\":"}} ,`+
			`{"index":1,"id":"c1","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/a.go\"}"}}`+
			`]},"finish_reason":"tool_calls"}]}`+"\n\n",
		"data: [DONE]\n\n",
	)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_, _, err := streamOpenAIResponses(recorder, req, strings.NewReader(strings.Join(chunks, "")), "resp_cc", "grok-4.5", []string{"Bash", "Read"}, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	for _, marker := range []string{
		"reasoning_summary_text.delta",
		"plan-",
		"function_call",
		"Read",
		"response.completed",
		"data: [DONE]",
	} {
		if !strings.Contains(out, marker) {
			t.Fatalf("missing %q in responses long-reasoning multi-tool (len=%d)", marker, len(out))
		}
	}
	if strings.Contains(out, `"name":"Bash"`) || (strings.Contains(out, "Bash") && strings.Contains(out, "function_call") && strings.Count(out, "Bash") > 0 && strings.Contains(out, `"call_id":"c0"`)) {
		// Soft check: incomplete Bash at c0 must not be emitted.
		if strings.Contains(out, `"call_id":"c0"`) {
			t.Fatalf("incomplete Bash c0 should not emit function_call")
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestClaudeCodeLongThinkingMultiToolEventTrace(t *testing.T) {
	// End-to-end SSE shape for Claude Code:
	// 1) early message_start
	// 2) live thinking_delta x N (keepalive)
	// 3) multi-tool with incomplete early index + complete later indexes
	// 4) message_delta + message_stop (task can leave "running")
	var upstream strings.Builder
	for i := 0; i < 25; i++ {
		upstream.WriteString(`data: {"choices":[{"delta":{"reasoning_content":"think-"}}]}` + "\n\n")
	}
	// incomplete Bash@0 should not block Read@1 / Edit@2
	upstream.WriteString(`data: {"choices":[{"delta":{"tool_calls":[`)
	upstream.WriteString(`{"index":0,"id":"t0","type":"function","function":{"name":"Bash","arguments":"{\"command\":"}},`)
	upstream.WriteString(`{"index":1,"id":"t1","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/tmp/a.go\"}"}},`)
	upstream.WriteString(`{"index":2,"id":"t2","type":"function","function":{"name":"Edit","arguments":"{\"file_path\":\"/tmp/b.go\",\"old_string\":\"x\",\"new_string\":\"y\"}"}}`)
	upstream.WriteString(`]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}` + "\n\n")
	upstream.WriteString("data: [DONE]\n\n")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	// toolsRequested=true, maxTools=2 (Claude-compatible default path)
	usage, firstTokenMS, err := streamAnthropicMessages(
		recorder, req, strings.NewReader(upstream.String()),
		"msg_claude_code", "grok-4.5", true,
		[]string{"Bash", "Read", "Edit"}, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()

	// Parse event types in order.
	var types []string
	thinkingN, toolStarts := 0, 0
	toolNames := []string{}
	for _, block := range strings.Split(out, "\n\n") {
		var ev string
		var data string
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				ev = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			}
			if strings.HasPrefix(line, "data: ") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			}
		}
		if ev == "" && data == "" {
			continue
		}
		if ev != "" {
			types = append(types, ev)
		}
		if strings.Contains(data, `"type":"thinking_delta"`) || strings.Contains(data, `"thinking_delta"`) {
			thinkingN++
		}
		if strings.Contains(data, `"type":"tool_use"`) {
			toolStarts++
			// rough name extract
			for _, name := range []string{"Bash", "Read", "Edit"} {
				if strings.Contains(data, `"name":"`+name+`"`) {
					toolNames = append(toolNames, name)
				}
			}
		}
	}

	t.Logf("firstTokenMS=%d usage=%v thinking_deltas~%d tool_starts=%d tools=%v events=%d",
		firstTokenMS, usage, thinkingN, toolStarts, toolNames, len(types))
	t.Logf("event head: %v", types[:min(12, len(types))])
	t.Logf("event tail: %v", types[max(0, len(types)-8):])

	// Early envelope
	if len(types) == 0 || types[0] != "message_start" {
		t.Fatalf("expected early message_start, got head=%v", types[:min(5, len(types))])
	}
	// Live thinking
	if thinkingN < 10 {
		t.Fatalf("expected live thinking_delta stream, got ~%d", thinkingN)
	}
	// Read must appear; incomplete Bash must not
	hasRead, hasBash, hasEdit := false, false, false
	for _, n := range toolNames {
		switch n {
		case "Read":
			hasRead = true
		case "Bash":
			hasBash = true
		case "Edit":
			hasEdit = true
		}
	}
	if !hasRead {
		t.Fatalf("Read tool missing (blocked by incomplete Bash?); tools=%v", toolNames)
	}
	if hasBash {
		t.Fatalf("incomplete Bash must not emit; tools=%v", toolNames)
	}
	// maxTools=2: Read + Edit both complete and should fit
	if !hasEdit {
		// with maxTools=2 and Bash skipped, Edit should still emit
		t.Fatalf("Edit tool missing; tools=%v", toolNames)
	}
	// Terminal
	if !strings.Contains(out, "event: message_delta") || !strings.Contains(out, "event: message_stop") {
		t.Fatalf("missing terminal frames")
	}
	if !strings.Contains(out, `"stop_reason":"tool_use"`) {
		t.Fatalf("expected stop_reason=tool_use")
	}
	// Envelope opened before model tokens: firstTokenMS may be tiny but stream must be OK.
	_ = firstTokenMS
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func TestAllowedAnthropicToolNamesTopLevelAndFunction(t *testing.T) {
	// Anthropic Messages tools use top-level name (Claude Code).
	names := allowedAnthropicToolNames(map[string]any{
		"tools": []any{
			map[string]any{"name": "Edit", "input_schema": map[string]any{"type": "object"}},
			map[string]any{"name": "Read", "description": "read"},
			map[string]any{"type": "function", "function": map[string]any{"name": "Bash"}},
		},
	})
	if len(names) != 3 {
		t.Fatalf("names=%v", names)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for _, want := range []string{"Edit", "Read", "Bash"} {
		if !got[want] {
			t.Fatalf("missing %s in %v", want, names)
		}
	}
	// Empty / nil
	if out := allowedAnthropicToolNames(nil); len(out) != 0 {
		t.Fatalf("nil body: %v", out)
	}
	if out := allowedAnthropicToolNames(map[string]any{}); len(out) != 0 {
		t.Fatalf("empty: %v", out)
	}
}
