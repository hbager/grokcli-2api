package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

type blockingCloseBody struct {
	started chan struct{}
	done    chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingCloseBody() *blockingCloseBody {
	return &blockingCloseBody{started: make(chan struct{}), done: make(chan struct{}), closed: make(chan struct{})}
}

func (b *blockingCloseBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.done
	return 0, io.EOF
}

func (b *blockingCloseBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
		close(b.done)
	}
	return nil
}

func TestResponsesToChatStreamCloseClosesUpstreamBody(t *testing.T) {
	upstream := newBlockingCloseBody()
	bridged := responsesToChatStream(upstream)
	select {
	case <-upstream.started:
	case <-time.After(time.Second):
		t.Fatal("bridge did not start reading upstream")
	}
	if err := bridged.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-upstream.closed:
	case <-time.After(time.Second):
		t.Fatal("closing bridge did not close upstream body")
	}
}

type closeCountingBody struct {
	*strings.Reader
	closes atomic.Int32
}

func (b *closeCountingBody) Close() error {
	b.closes.Add(1)
	return nil
}

func TestResponsesToChatStreamClosesUpstreamOnce(t *testing.T) {
	upstream := &closeCountingBody{Reader: strings.NewReader("data: [DONE]\n\n")}
	bridged := responsesToChatStream(upstream)
	if _, err := io.ReadAll(bridged); err != nil {
		t.Fatal(err)
	}
	if err := bridged.Close(); err != nil {
		t.Fatal(err)
	}
	if got := upstream.closes.Load(); got != 1 {
		t.Fatalf("upstream close count=%d want 1", got)
	}
}

func TestOpenUsesResponsesPathAndBridgesChatChunks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			t.Fatalf("path=%s want /v1/responses", r.URL.Path)
		}
		for name, want := range map[string]string{
			"Authorization":         "Bearer token",
			"X-Xai-Token-Auth":      "xai-grok-cli",
			"X-Grok-Client-Version": "0.2.93",
			"X-Grok-Conv-Id":        "sess-abc",
		} {
			if got := r.Header.Get(name); got != want {
				t.Errorf("%s=%q want %q", name, got, want)
			}
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != true {
			t.Fatalf("stream not forced: %#v", body)
		}
		if body["prompt_cache_key"] != "sess-abc" {
			t.Fatalf("prompt_cache_key not forwarded: %#v", body)
		}
		if _, ok := body["messages"]; ok {
			t.Fatalf("chat messages should be converted away: %#v", body)
		}
		input, _ := body["input"].([]any)
		if len(input) == 0 {
			t.Fatalf("expected responses input: %#v", body)
		}
		// Emit a minimal responses SSE stream.
		w.Header().Set("Content-Type", "text/event-stream")
		frames := []string{
			`event: response.created` + "\n" + `data: {"type":"response.created","response":{"id":"resp_1","model":"grok-4.5-build-free","created_at":1700000000}}` + "\n\n",
			`event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","delta":"hi"}` + "\n\n",
			`event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"resp_1","model":"grok-4.5-build-free","usage":{"input_tokens":10,"output_tokens":1,"total_tokens":11,"input_tokens_details":{"cached_tokens":7},"output_tokens_details":{"reasoning_tokens":2}}}}` + "\n\n",
		}
		for _, frame := range frames {
			_, _ = w.Write([]byte(frame))
		}
	}))
	defer server.Close()

	client := &Client{BaseURL: server.URL + "/v1", HTTP: server.Client()}
	response, err := client.Open(context.Background(), Account{ID: "a", Token: "token"}, "grok-4.5", map[string]any{
		"stream":           false,
		"prompt_cache_key": "sess-abc",
		"messages":         []any{map[string]any{"role": "user", "content": "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"object":"chat.completion.chunk"`) {
		t.Fatalf("expected bridged chat chunks, got %s", text)
	}
	if !strings.Contains(text, `"content":"hi"`) {
		t.Fatalf("missing content delta: %s", text)
	}
	if !strings.Contains(text, `"cached_tokens":7`) {
		t.Fatalf("missing cached tokens in bridged usage: %s", text)
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatalf("missing DONE: %s", text)
	}
	if response.Header.Get("X-Grok2API-Upstream-Protocol") != "responses" {
		t.Fatalf("missing upstream protocol header: %v", response.Header)
	}
}

func TestOpenUsesDynamicAccountStickyOutboundProxy(t *testing.T) {
	writeSSE := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}
	var proxyACalls atomic.Int64
	proxyA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyACalls.Add(1)
		writeSSE(w)
	}))
	defer proxyA.Close()
	var proxyBCalls atomic.Int64
	proxyB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyBCalls.Add(1)
		writeSSE(w)
	}))
	defer proxyB.Close()
	var directCalls atomic.Int64
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		directCalls.Add(1)
		writeSSE(w)
	}))
	defer direct.Close()

	runtime := config.NewRuntimeConfig(config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           proxyA.URL + "\n" + proxyB.URL,
		OutboundProxyStrategy:   "round_robin",
	})
	selector := outboundproxy.New(runtime.Load)
	client := &Client{BaseURL: direct.URL + "/v1", HTTP: NewHTTPClient(selector.Proxy)}
	open := func() {
		response, err := client.Open(t.Context(), Account{ID: "account-1", Token: "token"}, "grok-4.5", map[string]any{
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	open()
	open()
	if proxyACalls.Load()+proxyBCalls.Load() != 2 || (proxyACalls.Load() != 2 && proxyBCalls.Load() != 2) {
		t.Fatalf("proxy calls a=%d b=%d", proxyACalls.Load(), proxyBCalls.Load())
	}
	if directCalls.Load() != 0 {
		t.Fatalf("unexpected direct calls=%d", directCalls.Load())
	}

	runtime.ApplyStoreSettings(map[string]any{"outbound_proxy_config": map[string]any{
		"enabled": false,
		"proxy":   proxyA.URL + "\n" + proxyB.URL,
	}})
	open()
	if directCalls.Load() != 1 || proxyACalls.Load()+proxyBCalls.Load() != 2 {
		t.Fatalf("after disable direct=%d proxyA=%d proxyB=%d", directCalls.Load(), proxyACalls.Load(), proxyBCalls.Load())
	}
}

func TestClientConcurrentFirstOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	client := &Client{BaseURL: server.URL}
	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := client.Open(
				context.Background(),
				Account{Token: "token"},
				"grok-4.5",
				map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}},
			)
			if err != nil {
				errs <- err
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			if err := response.Body.Close(); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestChatToResponsesPayloadConvertsTools(t *testing.T) {
	body := chatToResponsesPayload(map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{
				"role": "assistant",
				"tool_calls": []any{
					map[string]any{
						"id":   "call_1",
						"type": "function",
						"function": map[string]any{
							"name":      "get_time",
							"arguments": `{"tz":"UTC"}`,
						},
					},
				},
			},
			map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "12:00"},
		},
		"tools": []any{
			map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        "get_time",
					"description": "t",
					"parameters":  map[string]any{"type": "object", "properties": map[string]any{}},
				},
			},
		},
		"max_tokens":       16,
		"reasoning_effort": "high",
		"prompt_cache_key": "pck",
		"stream_options":   map[string]any{"include_usage": true},
		"presence_penalty": 0.5,
	}, "grok-4.5")
	if body["max_output_tokens"] != 16 {
		t.Fatalf("max_output_tokens=%#v", body["max_output_tokens"])
	}
	if _, ok := body["stream_options"]; ok {
		t.Fatalf("stream_options should be dropped: %#v", body)
	}
	if _, ok := body["presence_penalty"]; ok {
		t.Fatalf("presence_penalty should be dropped: %#v", body)
	}
	reasoning, _ := body["reasoning"].(map[string]any)
	if reasoning["effort"] != "high" {
		t.Fatalf("reasoning=%#v", body["reasoning"])
	}
	tools, _ := body["tools"].([]any)
	if len(tools) < 2 {
		t.Fatalf("tools=%#v", body["tools"])
	}
	// x_search is prepended for cache routing even when client tools exist.
	first, _ := tools[0].(map[string]any)
	if first["type"] != "x_search" {
		t.Fatalf("default x_search missing: %#v", tools)
	}
	foundFn := false
	for _, item := range tools {
		tool, _ := item.(map[string]any)
		if tool["name"] == "get_time" && tool["type"] == "function" {
			foundFn = true
			if _, ok := tool["function"]; ok {
				t.Fatalf("nested function should be flattened: %#v", tool)
			}
		}
	}
	if !foundFn {
		t.Fatalf("function tool missing: %#v", tools)
	}
	input, _ := body["input"].([]any)
	if len(input) < 3 {
		t.Fatalf("input=%#v", input)
	}
	// assistant tool call becomes function_call item
	foundCall := false
	foundOut := false
	for _, item := range input {
		m, _ := item.(map[string]any)
		if m["type"] == "function_call" && m["name"] == "get_time" {
			foundCall = true
		}
		if m["type"] == "function_call_output" && m["call_id"] == "call_1" {
			foundOut = true
		}
	}
	if !foundCall || !foundOut {
		t.Fatalf("input missing tool roundtrip: %#v", input)
	}
}

func TestChatToResponsesPayloadPreservesImageURL(t *testing.T) {
	body := chatToResponsesPayload(map[string]any{
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "describe"},
					map[string]any{
						"type": "image_url",
						"image_url": map[string]any{
							"url":    "data:image/png;base64,abc",
							"detail": "high",
						},
					},
				},
			},
		},
	}, "grok-4.5")

	input, _ := body["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("input=%#v", body["input"])
	}
	message, _ := input[0].(map[string]any)
	parts, _ := message["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("content=%#v", message["content"])
	}
	text, _ := parts[0].(map[string]any)
	if text["type"] != "input_text" || text["text"] != "describe" {
		t.Fatalf("text=%#v", text)
	}
	image, _ := parts[1].(map[string]any)
	if image["type"] != "input_image" || image["image_url"] != "data:image/png;base64,abc" || image["detail"] != "high" {
		t.Fatalf("image=%#v", image)
	}
}

func TestChatToResponsesPayloadPreservesImageOnlyMessage(t *testing.T) {
	body := chatToResponsesPayload(map[string]any{
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{
						"type":      "image_url",
						"image_url": map[string]any{"url": "https://example.com/image.png"},
					},
				},
			},
		},
	}, "grok-4.5")

	input, _ := body["input"].([]any)
	message, _ := input[0].(map[string]any)
	parts, _ := message["content"].([]any)
	if len(parts) != 1 {
		t.Fatalf("content=%#v", message["content"])
	}
	image, _ := parts[0].(map[string]any)
	if image["type"] != "input_image" || image["image_url"] != "https://example.com/image.png" {
		t.Fatalf("image=%#v", image)
	}
}

func TestResponsesUsageToChatMapsCache(t *testing.T) {
	out := responsesUsageToChat(map[string]any{
		"input_tokens":  100,
		"output_tokens": 5,
		"total_tokens":  105,
		"input_tokens_details": map[string]any{
			"cached_tokens": 40,
		},
		"output_tokens_details": map[string]any{
			"reasoning_tokens": 3,
		},
	})
	if out["prompt_tokens"] != float64(100) || out["completion_tokens"] != float64(5) {
		t.Fatalf("tokens %#v", out)
	}
	details, _ := out["prompt_tokens_details"].(map[string]any)
	if details["cached_tokens"] != float64(40) {
		t.Fatalf("details %#v", details)
	}
	if out["cached_tokens"] != float64(40) {
		t.Fatalf("top cached %#v", out["cached_tokens"])
	}
}

func TestExtractConvIDPrefersPromptCacheKey(t *testing.T) {
	got := extractConvID(map[string]any{
		"prompt_cache_key": "pck",
		"conversation_id":  "conv",
		"metadata":         map[string]any{"session_id": "sess"},
	})
	if got != "pck" {
		t.Fatalf("got %q", got)
	}
	got = extractConvID(map[string]any{"metadata": map[string]any{"session_id": "sess"}})
	if got != "sess" {
		t.Fatalf("meta session got %q", got)
	}
}

func TestReadSSE(t *testing.T) {
	var events []Event
	err := ReadSSE(strings.NewReader("data: {\"x\":1}\n\ndata: [DONE]\n\n"), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || len(events) != 2 || events[0].Done || !events[1].Done {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}

func TestChatToResponsesPayloadDefaultsXSearchTool(t *testing.T) {
	body := chatToResponsesPayload(map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, "grok-4.5")
	tools, _ := body["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools=%#v", body["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["type"] != "x_search" {
		t.Fatalf("default tool=%#v", tool)
	}
	input, _ := body["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("input=%#v", input)
	}
	msg, _ := input[0].(map[string]any)
	if msg["type"] != "message" || msg["role"] != "user" {
		t.Fatalf("msg=%#v", msg)
	}
}

func TestReadSSEWithIdleEarlyExitDrains(t *testing.T) {
	// Producer emits many frames; consumer aborts on first — must not hang/leak.
	var b strings.Builder
	for i := 0; i < 50; i++ {
		b.WriteString("data: {\"i\":")
		b.WriteString(fmt.Sprintf("%d", i))
		b.WriteString("}\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	n := 0
	err := ReadSSEWithIdle(strings.NewReader(b.String()), 50*time.Millisecond, func(event Event) error {
		n++
		return errors.New("client gone")
	}, func() error { return nil })
	if err == nil || err.Error() != "client gone" {
		t.Fatalf("err=%v", err)
	}
	if n != 1 {
		t.Fatalf("emitted %d, want 1", n)
	}
	// Give drain goroutine a tick; if it leaked blocked on send this would still pass,
	// but at least we cover the early-exit path without hanging the test.
	time.Sleep(20 * time.Millisecond)
}

func TestReadSSEWithIdleKeepalive(t *testing.T) {
	// Blocked reader: onIdle must fire, then stream completes.
	pr, pw := io.Pipe()
	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = pw.Write([]byte("data: {\"x\":1}\n\n"))
		time.Sleep(10 * time.Millisecond)
		_, _ = pw.Write([]byte("data: [DONE]\n\n"))
		_ = pw.Close()
	}()
	idleN := 0
	got := 0
	err := ReadSSEWithIdle(pr, 15*time.Millisecond, func(event Event) error {
		if !event.Done {
			got++
		}
		return nil
	}, func() error {
		idleN++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("got events=%d", got)
	}
	if idleN < 1 {
		t.Fatalf("expected keepalive idle ticks, got %d", idleN)
	}
}
