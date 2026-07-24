package web

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

// WebToChatStream converts Grok Web object stream into chat.completion.chunk SSE.
func WebToChatStream(src io.ReadCloser, model string) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		defer src.Close()
		err := bridgeWebToChat(src, pw, model)
		_ = pw.CloseWithError(err)
	}()
	return pr
}

func bridgeWebToChat(src io.Reader, dst *io.PipeWriter, model string) error {
	model = strings.TrimSpace(model)
	if model == "" {
		model = "grok-chat-fast"
	}
	id := "chatcmpl-web-" + hex.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))[:12]
	created := time.Now().Unix()
	writeChunk := func(delta map[string]any, finish any) error {
		chunk := map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
		}
		raw, _ := json.Marshal(chunk)
		_, err := fmt.Fprintf(dst, "data: %s\n\n", raw)
		return err
	}
	if err := writeChunk(map[string]any{"role": "assistant"}, nil); err != nil {
		return err
	}
	var mu sync.Mutex
	var text strings.Builder
	var upstreamText strings.Builder
	emittedAny := false
	emitText := func(delta string) error {
		delta = strings.TrimRight(delta, "\r")
		if delta == "" {
			return nil
		}
		mu.Lock()
		text.WriteString(delta)
		emittedAny = true
		mu.Unlock()
		return writeChunk(map[string]any{"content": delta}, nil)
	}
	err := consumeJSONObjects(src, 8<<20, func(data []byte) error {
		var root map[string]any
		if json.Unmarshal(data, &root) != nil {
			return nil
		}
		if errVal, ok := root["error"].(map[string]any); ok {
			msg, _ := errVal["message"].(string)
			if msg == "" {
				msg = "web upstream error"
			}
			return &grok.UpstreamError{Status: 502, Body: msg}
		}
		result, _ := root["result"].(map[string]any)
		if result == nil {
			return nil
		}
		response, _ := result["response"].(map[string]any)
		if response == nil {
			return nil
		}
		if errVal, ok := response["error"].(map[string]any); ok {
			msg, _ := errVal["message"].(string)
			if msg == "" {
				msg = "web response error"
			}
			low := strings.ToLower(msg)
			status := 502
			if strings.Contains(low, "anti-bot") {
				status = 403
			} else if strings.Contains(low, "usage limit") || strings.Contains(low, "usage quota") {
				status = 429
			}
			return &grok.UpstreamError{Status: status, Body: msg}
		}
		token, _ := response["token"].(string)
		thinking, _ := response["isThinking"].(bool)
		tag, _ := response["messageTag"].(string)
		if token != "" && !thinking && (tag == "final" || tag == "") {
			upstreamText.WriteString(token)
			return emitText(token)
		}
		if modelResponse, _ := response["modelResponse"].(map[string]any); modelResponse != nil {
			if errs, _ := modelResponse["streamErrors"].([]any); len(errs) > 0 {
				return &grok.UpstreamError{Status: 502, Body: fmt.Sprint(errs[0])}
			}
			message, _ := modelResponse["message"].(string)
			if message == "" {
				return nil
			}
			raw := upstreamText.String()
			if raw == message || strings.HasPrefix(raw, message) {
				return nil
			}
			if raw != "" && !strings.HasPrefix(message, raw) {
				return nil
			}
			delta := message[len(raw):]
			upstreamText.WriteString(delta)
			return emitText(delta)
		}
		return nil
	})
	if err != nil {
		return err
	}
	mu.Lock()
	has := emittedAny || text.Len() > 0
	mu.Unlock()
	if !has {
		return &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	if err := writeChunk(map[string]any{}, "stop"); err != nil {
		return err
	}
	_, err = io.WriteString(dst, "data: [DONE]\n\n")
	return err
}

func consumeJSONObjects(source io.Reader, maxObjectBytes int, consume func([]byte) error) error {
	reader := bufio.NewReaderSize(source, 64<<10)
	frame := make([]byte, 0, 64<<10)
	depth := 0
	inString := false
	escaped := false
	for {
		value, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				if depth != 0 {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return err
		}
		if depth == 0 {
			if value != '{' && value != '[' {
				continue
			}
		}
		frame = append(frame, value)
		if maxObjectBytes > 0 && len(frame) > maxObjectBytes {
			return fmt.Errorf("web stream object exceeds %d bytes", maxObjectBytes)
		}
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if value == '\\' {
				escaped = true
				continue
			}
			if value == '"' {
				inString = false
			}
			continue
		}
		switch value {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				data := append([]byte(nil), frame...)
				frame = frame[:0]
				if err := consume(data); err != nil {
					return err
				}
			}
		}
	}
}
