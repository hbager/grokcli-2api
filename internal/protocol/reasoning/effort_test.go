package reasoning

import "testing"

func TestNormalizeAliases(t *testing.T) {
	// Grok only has low|medium|high. Client 4-tier labels fold onto those 3.
	cases := map[string]string{
		"":         "",
		"none":     "",
		"disabled": "",
		// Claude Code
		"low":    Low,
		"medium": Medium,
		"high":   High,
		"xhigh":  High, // CC top tier → Grok high (no 4th tier)
		// Codex
		"auto":       Low,
		"default":    Medium,
		"standard":   High,
		"extra-high": High,
		"extra_high": High,
		"extra high": High,
		"extrahigh":  High,
		"max":        High,
		"maximum":    High,
		"ultra":      High,
		// misc
		"enabled":  Medium,
		"adaptive": Medium,
		"minimal":  Low,
		"MIN":      Low,
		"HIGH":     High,
		"XHIGH":    High,
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
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 50000}); got != High {
		t.Fatalf("budget top = %q want high (Grok 3-tier)", got)
	}
	if got := Normalize(map[string]any{"type": "auto"}); got != Low {
		t.Fatalf("type auto = %q", got)
	}
	if got := Normalize(map[string]any{"type": "standard"}); got != High {
		t.Fatalf("type standard = %q", got)
	}
	if got := Normalize(map[string]any{"effort": "extra-high"}); got != High {
		t.Fatalf("effort extra-high = %q want high", got)
	}
	if got := Normalize(map[string]any{"type": "disabled"}); got != "" {
		t.Fatalf("disabled = %q", got)
	}
}

func TestFromRequest(t *testing.T) {
	if got := FromRequest(map[string]any{"reasoning_effort": "auto"}); got != Low {
		t.Fatalf("auto = %q", got)
	}
	if got := FromRequest(map[string]any{"reasoning": map[string]any{"effort": "extra-high"}}); got != High {
		t.Fatalf("nested extra-high = %q want high", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 9000}}); got != High {
		t.Fatalf("thinking = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": "standard"}); got != High {
		t.Fatalf("thinking string = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"effort": "xhigh"}}); got != High {
		t.Fatalf("claude xhigh = %q want high", got)
	}
}

func TestApplyCanonical(t *testing.T) {
	body := map[string]any{"reasoning_effort": "extra_high"}
	if got := ApplyCanonical(body); got != High || body["reasoning_effort"] != High {
		t.Fatalf("got %q body=%v", got, body)
	}
	body = map[string]any{"reasoning": map[string]any{"effort": "default"}}
	if got := ApplyCanonical(body); got != Medium || body["reasoning_effort"] != Medium {
		t.Fatalf("got %q body=%v", got, body)
	}
	// Never emit the string "xhigh" upstream.
	body = map[string]any{"reasoning_effort": "xhigh"}
	if got := ApplyCanonical(body); got != High || body["reasoning_effort"] != "high" {
		t.Fatalf("xhigh must fold to high, got %q body=%v", got, body)
	}
}

func TestNeverEmitsXHighString(t *testing.T) {
	for _, in := range []any{
		"xhigh", "XHIGH", "extra-high", "max", "ultra",
		map[string]any{"effort": "xhigh"},
		map[string]any{"type": "enabled", "budget_tokens": 999999},
	} {
		got := Normalize(in)
		if got == "xhigh" {
			t.Fatalf("must not emit xhigh for %v", in)
		}
		if got != "" && got != Low && got != Medium && got != High {
			t.Fatalf("Normalize(%v)=%q not in {low,medium,high}", in, got)
		}
	}
}
