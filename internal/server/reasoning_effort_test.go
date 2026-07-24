package server

import "testing"

func TestExtractReasoningEffort(t *testing.T) {
	cases := []struct {
		in   map[string]any
		want string
	}{
		// Client-facing labels (usage detail / admin UI)
		{map[string]any{"reasoning_effort": "high"}, "high"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 8000}}, "medium"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 50000}}, "xhigh"},
		{map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 200000}}, "max"},
		{map[string]any{"reasoning": map[string]any{"effort": "low"}}, "low"},
		{map[string]any{"effort": "MAX"}, "max"},
		// Claude Code / Anthropic output_config.effort
		{map[string]any{"output_config": map[string]any{"effort": "low"}}, "low"},
		{map[string]any{"output_config": map[string]any{"effort": "medium"}}, "medium"},
		{map[string]any{"output_config": map[string]any{"effort": "high"}}, "high"},
		{map[string]any{"output_config": map[string]any{"effort": "xhigh"}}, "xhigh"},
		{map[string]any{"output_config": map[string]any{"effort": "max"}}, "max"},
		{map[string]any{"output_config": map[string]any{"effort": "ultracode"}}, "ultracode"},
		// Codex thinking modes → client labels
		// Low / Base / High / Ultra / Proactive (+ legacy aliases)
		{map[string]any{"reasoning_effort": "auto"}, "low"},
		{map[string]any{"reasoning_effort": "low"}, "low"},
		{map[string]any{"reasoning_effort": "base"}, "medium"},
		{map[string]any{"reasoning_effort": "default"}, "medium"},
		{map[string]any{"reasoning_effort": "high"}, "high"},
		{map[string]any{"reasoning_effort": "proactive"}, "high"},
		{map[string]any{"reasoning_effort": "standard"}, "high"},
		{map[string]any{"reasoning_effort": "ultra"}, "ultracode"},
		{map[string]any{"reasoning_effort": "extra-high"}, "xhigh"},
		{map[string]any{"thinking": "xhigh"}, "xhigh"},
		{map[string]any{"reasoning": map[string]any{"effort": "extra_high"}}, "xhigh"},
		{map[string]any{"reasoning": map[string]any{"effort": "proactive"}}, "high"},
		{map[string]any{"effort": "ultracode"}, "ultracode"},
		{map[string]any{}, ""},
	}
	for i, tc := range cases {
		got := extractReasoningEffort(tc.in)
		if got != tc.want {
			t.Fatalf("case %d: got %q want %q (in=%v)", i, got, tc.want, tc.in)
		}
	}
}

func TestUsageDetailRecordsPublicModelUpstreamAndFixedEffort(t *testing.T) {
	// Alias forces xhigh even when request body has no effort field.
	d := usageDetail("go_chat", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, 12, 100, "console/grok-4.20-multi-agent-xhigh")
	if d["upstream_model"] != "grok-4.20-multi-agent" {
		t.Fatalf("upstream_model=%v", d["upstream_model"])
	}
	if d["model"] != "console/grok-4.20-multi-agent-xhigh" {
		t.Fatalf("model=%v", d["model"])
	}
	if d["reasoning_effort"] != "xhigh" || d["thinking_intensity"] != "xhigh" {
		t.Fatalf("effort fields=%#v", d)
	}
	if d["provider"] != "console" {
		t.Fatalf("provider=%v", d["provider"])
	}
}

func TestUsageDetailRequestEffortWinsOverEmptyFixed(t *testing.T) {
	d := usageDetail("go_chat", map[string]any{
		"reasoning_effort": "high",
		"messages":         []any{},
	}, 0, 50, "grok-4.5")
	if d["reasoning_effort"] != "high" || d["thinking_intensity"] != "high" {
		t.Fatalf("%#v", d)
	}
	if d["upstream_model"] != "grok-4.5" {
		t.Fatalf("upstream=%v", d["upstream_model"])
	}
}

func TestUsageDetailFixedEffortWinsWhenBodyOmitsEffort(t *testing.T) {
	// Body may still carry unrelated fields; fixed route effort must fill logs.
	d := usageDetail("go_chat", map[string]any{"stream": true}, 1, 2, "console/grok-4.20-multi-agent-xhigh")
	if d["reasoning_effort"] != "xhigh" {
		t.Fatalf("want fixed xhigh, got %#v", d)
	}
}
