package server

import "testing"

func TestExtractReasoningEffort(t *testing.T) {
	cases := []struct {
		in   map[string]any
		want string
	}{
		{map[string]any{"reasoning_effort": "high"}, "high"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 8000}}, "medium"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 50000}}, "xhigh"}, // top budget -> xhigh
		{map[string]any{"reasoning": map[string]any{"effort": "low"}}, "low"},
		{map[string]any{"effort": "MAX"}, "xhigh"}, // max/extra-high -> xhigh
		// Codex / Claude aliases -> Grok 4-tier
		{map[string]any{"reasoning_effort": "auto"}, "low"},
		{map[string]any{"reasoning_effort": "default"}, "medium"},
		{map[string]any{"reasoning_effort": "standard"}, "high"},
		{map[string]any{"reasoning_effort": "extra-high"}, "xhigh"},
		{map[string]any{"thinking": "xhigh"}, "xhigh"},
		{map[string]any{"reasoning": map[string]any{"effort": "extra_high"}}, "xhigh"},
		{map[string]any{}, ""},
	}
	for i, tc := range cases {
		got := extractReasoningEffort(tc.in)
		if got != tc.want {
			t.Fatalf("case %d: got %q want %q", i, got, tc.want)
		}
	}
}
