// Package reasoning normalizes thinking / reasoning effort labels across clients.
//
// cli-chat-proxy (Grok upstream) accepts FOUR outbound levels:
//
//	low | medium | high | xhigh
//
// Client labels are folded onto those four:
//
//	Codex:        auto | default | standard | extra-high
//	Claude Code:  low  | medium  | high     | xhigh
//
//	auto / low / minimal     -> low
//	default / medium / med   -> medium
//	standard / high          -> high
//	extra-high / xhigh / max -> xhigh   (upstream accepts xhigh; rejects max/extra-high)
package reasoning

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Canonical levels emitted upstream (Grok 4-tier).
const (
	Low    = "low"
	Medium = "medium"
	High   = "high"
	XHigh  = "xhigh"
)

// Normalize maps free-form client effort labels (and budgets) to Grok's
// low|medium|high|xhigh. Empty means "no reasoning effort" (disabled/none).
func Normalize(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case bool:
		if v {
			return Medium
		}
		return ""
	case float64:
		return BudgetToLevel(int(v))
	case float32:
		return BudgetToLevel(int(v))
	case int:
		return BudgetToLevel(v)
	case int64:
		return BudgetToLevel(int(v))
	case int32:
		return BudgetToLevel(int(v))
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return ""
		}
		return BudgetToLevel(int(n))
	case string:
		return normalizeString(v)
	case map[string]any:
		// Prefer explicit effort fields, then budget, then type/enabled.
		for _, key := range []string{
			"effort", "reasoning_effort", "thinking_effort",
			"intensity", "level", "thinking_intensity",
		} {
			if vv, ok := v[key]; ok && vv != nil {
				if got := Normalize(vv); got != "" {
					return got
				}
			}
		}
		if vv, ok := v["budget_tokens"]; ok && vv != nil {
			if got := Normalize(vv); got != "" {
				return got
			}
		}
		tt := strings.ToLower(strings.TrimSpace(fmt.Sprint(v["type"])))
		switch tt {
		case "", "disabled", "none", "false", "off", "0":
			// fall through to enabled flag
		case Low, Medium, High, XHigh:
			return tt
		case "x-high":
			return XHigh
		case "enabled", "true", "on", "adaptive":
			if got := Normalize(v["budget_tokens"]); got != "" {
				return got
			}
			return Medium
		case "auto", "default", "standard":
			// Codex-style type labels map through the same table.
			return normalizeString(tt)
		default:
			if got := normalizeString(tt); got != "" {
				return got
			}
		}
		if v["enabled"] == true {
			return Medium
		}
		return ""
	default:
		return normalizeString(fmt.Sprint(v))
	}
}

func normalizeString(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return ""
	}
	// normalize separators: extra-high / extra_high / extra high
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.Join(strings.Fields(s), "-")

	switch s {
	case "none", "null", "false", "off", "disabled", "0", "no":
		return ""
	// low tier: Claude Code low, Codex auto, misc minimal/fast
	case Low, "minimal", "min", "l", "lite", "fast", "auto":
		return Low
	// medium tier: Claude Code medium, Codex default
	case Medium, "default", "normal", "balanced", "mid", "m", "med",
		"adaptive", "enabled", "true", "on", "1":
		return Medium
	// high tier: Claude Code high, Codex standard
	case High, "standard", "std", "h", "hard", "deep":
		return High
	// xhigh tier (upstream top). Accepts "xhigh" only; fold aliases.
	// Note: upstream rejects literal "max"/"extra-high"; fold them to xhigh.
	case XHigh, "x-high", "extra-high", "extrahigh", "extra",
		"max", "maximum", "maxi", "ultra", "ultra-high", "ultrahigh",
		"highest", "maxx":
		return XHigh
	}
	if strings.HasPrefix(s, "extra") && strings.Contains(s, "high") {
		return XHigh
	}
	// Unknown non-empty labels: do not pass garbage upstream.
	return ""
}

// BudgetToLevel maps Claude-style thinking.budget_tokens onto Grok's 4 tiers.
func BudgetToLevel(n int) string {
	if n <= 0 {
		return ""
	}
	if n <= 2048 {
		return Low
	}
	if n <= 8192 {
		return Medium
	}
	if n <= 24576 {
		return High
	}
	// Very large budgets -> xhigh (upstream top tier).
	return XHigh
}

// FromRequest extracts effort from a chat/completions or Responses-shaped body.
// Looks at reasoning_effort, reasoning.effort, thinking, thinking_effort, etc.
func FromRequest(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	for _, key := range []string{"reasoning_effort", "thinking_effort", "effort", "thinking_intensity"} {
		if v, ok := raw[key]; ok && v != nil {
			if got := Normalize(v); got != "" {
				return got
			}
		}
	}
	if v, ok := raw["reasoning"]; ok && v != nil {
		if got := Normalize(v); got != "" {
			return got
		}
	}
	if v, ok := raw["thinking"]; ok && v != nil {
		if got := Normalize(v); got != "" {
			return got
		}
	}
	if text, ok := raw["text"].(map[string]any); ok {
		if got := FromRequest(text); got != "" {
			return got
		}
	}
	return ""
}

// ApplyCanonical writes a normalized reasoning_effort into body when present.
// Returns the canonical level (may be empty). Always low|medium|high|xhigh.
func ApplyCanonical(body map[string]any) string {
	if body == nil {
		return ""
	}
	effort := FromRequest(body)
	if effort == "" {
		if v, ok := body["reasoning_effort"]; ok {
			effort = Normalize(v)
		}
	}
	if effort == "" {
		return ""
	}
	body["reasoning_effort"] = effort
	return effort
}
