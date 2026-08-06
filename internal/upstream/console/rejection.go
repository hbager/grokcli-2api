package console

import (
	"encoding/json"
	"strings"
)

const dpopRequiredCode = "unauthorized:dpop-required"

var dpopProtocolMessages = []string{
	"dpop proof required but was not verified",
}

// IsDPoPProtocolRejection reports whether a Console error describes a failure
// to provide or verify the DPoP proof, rather than an invalid SSO session.
func IsDPoPProtocolRejection(errText string) bool {
	errText = strings.TrimSpace(errText)
	if errText == "" {
		return false
	}

	var payload any
	if err := json.Unmarshal([]byte(errText), &payload); err == nil {
		return hasDPoPProtocolMarker(payload)
	}
	return isDPoPProtocolMarker(errText)
}

func hasDPoPProtocolMarker(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "code", "error", "message", "detail", "description", "error_description":
				if hasDPoPProtocolMarker(child) {
					return true
				}
			}
		}
	case []any:
		for _, child := range typed {
			if hasDPoPProtocolMarker(child) {
				return true
			}
		}
	case string:
		return isDPoPProtocolMarker(typed)
	}
	return false
}

func isDPoPProtocolMarker(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == dpopRequiredCode {
		return true
	}
	for _, message := range dpopProtocolMessages {
		if strings.Contains(value, message) {
			return true
		}
	}
	return false
}
