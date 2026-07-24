package web

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ModeForModel maps public/upstream web chat ids to modeId.
func ModeForModel(model string) (string, bool) {
	m := strings.ToLower(strings.TrimSpace(model))
	m = strings.TrimPrefix(m, "web/")
	switch m {
	case "grok-chat-fast", "fast":
		return "fast", true
	case "grok-chat-auto", "auto":
		return "auto", true
	case "grok-chat-expert", "expert":
		return "expert", true
	case "grok-chat-heavy", "heavy":
		return "heavy", true
	default:
		return "", false
	}
}

func buildWebChatPayload(message, mode string) map[string]any {
	return map[string]any{
		"collectionIds": []any{}, "disabledConnectorIds": []any{},
		"deviceEnvInfo": map[string]any{"darkModeEnabled": false, "devicePixelRatio": 2, "screenHeight": 1328, "screenWidth": 2056, "viewportHeight": 1083, "viewportWidth": 2056},
		"disableMemory": true, "disableSearch": false, "disableSelfHarmShortCircuit": false,
		"disableTextFollowUps": false, "enableImageGeneration": false, "enableImageStreaming": false,
		"enableSideBySide": true, "fileAttachments": []string{}, "forceConcise": false,
		"forceSideBySide": false, "imageAttachments": []any{}, "imageGenerationCount": 0,
		"isAsyncChat": false, "message": message, "modeId": mode, "responseMetadata": map[string]any{},
		"returnImageBytes": false, "returnRawGrokInXaiRequest": false, "sendFinalMetadata": true, "temporary": true,
	}
}

func extractUserPrompt(body map[string]any) string {
	if body == nil {
		return ""
	}
	if raw, ok := body["messages"].([]any); ok {
		var parts []string
		var lastUser string
		for _, item := range raw {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			role, _ := m["role"].(string)
			text := contentToString(m["content"])
			if text == "" {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(role)) {
			case "system", "developer":
				parts = append(parts, text)
			case "user":
				lastUser = text
			}
		}
		if lastUser != "" {
			if len(parts) > 0 {
				return strings.Join(parts, "\n\n") + "\n\n" + lastUser
			}
			return lastUser
		}
	}
	if raw, ok := body["input"].([]any); ok {
		for i := len(raw) - 1; i >= 0; i-- {
			m, ok := raw[i].(map[string]any)
			if !ok {
				continue
			}
			role, _ := m["role"].(string)
			if strings.EqualFold(role, "user") {
				if t := contentToString(m["content"]); t != "" {
					return t
				}
			}
		}
	}
	if s, ok := body["input"].(string); ok {
		return strings.TrimSpace(s)
	}
	if s, ok := body["message"].(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func contentToString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case []any:
		var b strings.Builder
		for _, part := range t {
			switch p := part.(type) {
			case string:
				if strings.TrimSpace(p) != "" {
					if b.Len() > 0 {
						b.WriteByte('\n')
					}
					b.WriteString(strings.TrimSpace(p))
				}
			case map[string]any:
				typ, _ := p["type"].(string)
				if typ == "text" || typ == "input_text" || typ == "output_text" {
					if s, _ := p["text"].(string); strings.TrimSpace(s) != "" {
						if b.Len() > 0 {
							b.WriteByte('\n')
						}
						b.WriteString(strings.TrimSpace(s))
					}
				}
			}
		}
		return b.String()
	default:
		return ""
	}
}

func newRequestUUID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}
