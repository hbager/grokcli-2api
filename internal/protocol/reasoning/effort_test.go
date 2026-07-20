package reasoning

import "testing"

func TestNormalizeAliases(t *testing.T) {
	// Upstream accepts low|medium|high|xhigh. Client aliases fold onto those 4.
	cases := map[string]string{
		"":         "",
		"none":     "",
		"disabled": "",
		// Claude Code
		"low":    Low,
		"medium": Medium,
		"high":   High,
		"xhigh":  XHigh,
		// Codex
		"auto":       Low,
		"default":    Medium,
		"standard":   High,
		"extra-high": XHigh, // Codex top -> upstream xhigh (upstream rejects "extra-high")
		"extra_high": XHigh,
		"extra high": XHigh,
		"extrahigh":  XHigh,
		"max":        XHigh, // upstream rejects "max"; fold to xhigh
		"maximum":    XHigh,
		"ultra":      XHigh,
		// misc
		"enabled":  Medium,
		"adaptive": Medium,
		"minimal":  Low,
		"MIN":      Low,
		"HIGH":     High,
		"XHIGH":    XHigh,
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Fatalf("Normalize(%q)=%q want %q", in, got, want)
		}
	}
}

func TestNormalizeThinkingObject(t *testing.T) {
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 1000}); got != Low {
		t.Fatalf("budget low = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 4096}); got != Medium {
		t.Fatalf("budget med = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 9000}); got != High {
		t.Fatalf("budget high = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 50000}); got != XHigh {
		t.Fatalf("budget top = %q want xhigh", got)
	}
	if got := Normalize(map[string]any{"type": "auto"}); got != Low {
		t.Fatalf("type auto = %q", got)
	}
	if got := Normalize(map[string]any{"type": "standard"}); got != High {
		t.Fatalf("type standard = %q", got)
	}
	if got := Normalize(map[string]any{"effort": "extra-high"}); got != XHigh {
		t.Fatalf("effort extra-high = %q want xhigh", got)
	}
	if got := Normalize(map[string]any{"type": "disabled"}); got != "" {
		t.Fatalf("disabled = %q", got)
	}
}

func TestFromRequest(t *testing.T) {
	if got := FromRequest(map[string]any{"reasoning_effort": "auto"}); got != Low {
		t.Fatalf("auto = %q", got)
	}
	if got := FromRequest(map[string]any{"reasoning": map[string]any{"effort": "extra-high"}}); got != XHigh {
		t.Fatalf("nested extra-high = %q want xhigh", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 9000}}); got != High {
		t.Fatalf("thinking = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": "standard"}); got != High {
		t.Fatalf("thinking string = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"effort": "xhigh"}}); got != XHigh {
		t.Fatalf("claude xhigh = %q want xhigh", got)
	}
}

func TestApplyCanonical(t *testing.T) {
	body := map[string]any{"reasoning_effort": "extra_high"}
	if got := ApplyCanonical(body); got != XHigh || body["reasoning_effort"] != XHigh {
		t.Fatalf("got %q body=%v", got, body)
	}
	body = map[string]any{"reasoning": map[string]any{"effort": "default"}}
	if got := ApplyCanonical(body); got != Medium || body["reasoning_effort"] != Medium {
		t.Fatalf("got %q body=%v", got, body)
	}
	// Pass through xhigh upstream (cli-chat-proxy accepts it).
	body = map[string]any{"reasoning_effort": "xhigh"}
	if got := ApplyCanonical(body); got != XHigh || body["reasoning_effort"] != "xhigh" {
		t.Fatalf("xhigh must pass through, got %q body=%v", got, body)
	}
	// max is rejected by upstream; fold to xhigh before send.
	body = map[string]any{"reasoning_effort": "max"}
	if got := ApplyCanonical(body); got != XHigh || body["reasoning_effort"] != "xhigh" {
		t.Fatalf("max must fold to xhigh, got %q body=%v", got, body)
	}
}

func TestCanonicalLevelsOnly(t *testing.T) {
	for _, in := range []any{
		"xhigh", "XHIGH", "extra-high", "max", "ultra",
		map[string]any{"effort": "xhigh"},
		map[string]any{"type": "enabled", "budget_tokens": 999999},
	} {
		got := Normalize(in)
		if got != "" && got != Low && got != Medium && got != High && got != XHigh {
			t.Fatalf("Normalize(%v)=%q not in {low,medium,high,xhigh}", in, got)
		}
	}
	// Never emit labels upstream rejects.
	for _, bad := range []string{"max", "extra-high", "ultra", "maximum"} {
		if got := Normalize(bad); got == bad {
			t.Fatalf("must not emit rejected label %q", bad)
		}
	}
}
