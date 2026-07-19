package server

import "testing"

func TestExtractReasoningEffort(t *testing.T) {
	cases := []struct {
		in   map[string]any
		want string
	}{
		{map[string]any{"reasoning_effort": "high"}, "high"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 8000}}, "medium"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 50000}}, "high"}, // Grok 3-tier: top budget → high
		{map[string]any{"reasoning": map[string]any{"effort": "low"}}, "low"},
		{map[string]any{"effort": "MAX"}, "high"}, // max/extra-high/xhigh → high
		// Codex 4-tier → Grok 3-tier
		{map[string]any{"reasoning_effort": "auto"}, "low"},
		{map[string]any{"reasoning_effort": "default"}, "medium"},
		{map[string]any{"reasoning_effort": "standard"}, "high"},
		{map[string]any{"reasoning_effort": "extra-high"}, "high"},
		{map[string]any{"thinking": "xhigh"}, "high"},
		{map[string]any{"reasoning": map[string]any{"effort": "extra_high"}}, "high"},
		{map[string]any{}, ""},
	}
	for i, tc := range cases {
		got := extractReasoningEffort(tc.in)
		if got != tc.want {
			t.Fatalf("case %d: got %q want %q", i, got, tc.want)
		}
	}
}
