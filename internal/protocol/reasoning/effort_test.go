package reasoning

import "testing"

func TestNormalizeClientAliases(t *testing.T) {
	// Client labels preserve Claude Code 5 API tiers + ultracode UI mode.
	cases := map[string]string{
		"":         "",
		"none":     "",
		"disabled": "",
		// Claude Code / Anthropic API
		"low":       ClientLow,
		"medium":    ClientMedium,
		"high":      ClientHigh,
		"xhigh":     ClientXHigh,
		"x-high":    ClientXHigh,
		"max":       ClientMax,
		"maximum":   ClientMax,
		"ultracode": ClientUltracode,
		"ultra":     ClientUltracode,
		// Codex thinking modes: Low / Base / High / Ultra / Proactive
		// (+ legacy auto/default/standard/extra-high)
		"auto":       ClientLow,
		"base":       ClientMedium,
		"default":    ClientMedium,
		"standard":   ClientHigh,
		"proactive":  ClientHigh,
		"extra-high": ClientXHigh,
		"extra_high": ClientXHigh,
		"extra high": ClientXHigh,
		"extrahigh":  ClientXHigh,
		// misc
		"enabled":   ClientMedium,
		"adaptive":  ClientMedium,
		"minimal":   ClientLow,
		"MIN":       ClientLow,
		"HIGH":      ClientHigh,
		"XHIGH":     ClientXHigh,
		"MAX":       ClientMax,
		"ULTRACODE": ClientUltracode,
	}
	for in, want := range cases {
		if got := NormalizeClient(in); got != want {
			t.Fatalf("NormalizeClient(%q)=%q want %q", in, got, want)
		}
	}
}

func TestToUpstream(t *testing.T) {
	cases := map[string]string{
		"":           "",
		"low":        Low,
		"medium":     Medium,
		"high":       High,
		"xhigh":      XHigh,
		"max":        XHigh,
		"ultracode":  XHigh,
		"extra-high": XHigh,
		"Ultra":      XHigh,
		"auto":       Low,
		// Codex thinking modes → Grok 4-tier
		"base":      Medium,
		"proactive": High,
		"ultra":     XHigh, // Codex Ultra → ultracode client → xhigh upstream
	}
	for in, want := range cases {
		if got := ToUpstream(in); got != want {
			t.Fatalf("ToUpstream(%q)=%q want %q", in, got, want)
		}
	}
}

func TestNormalizeThinkingObject(t *testing.T) {
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 1000}); got != ClientLow {
		t.Fatalf("budget low = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 4096}); got != ClientMedium {
		t.Fatalf("budget med = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 9000}); got != ClientHigh {
		t.Fatalf("budget high = %q", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 50000}); got != ClientXHigh {
		t.Fatalf("budget xhigh = %q want xhigh", got)
	}
	if got := Normalize(map[string]any{"type": "enabled", "budget_tokens": 200000}); got != ClientMax {
		t.Fatalf("budget max = %q want max", got)
	}
	if got := Normalize(map[string]any{"type": "auto"}); got != ClientLow {
		t.Fatalf("type auto = %q", got)
	}
	if got := Normalize(map[string]any{"type": "standard"}); got != ClientHigh {
		t.Fatalf("type standard = %q", got)
	}
	if got := Normalize(map[string]any{"effort": "extra-high"}); got != ClientXHigh {
		t.Fatalf("effort extra-high = %q want xhigh", got)
	}
	if got := Normalize(map[string]any{"effort": "max"}); got != ClientMax {
		t.Fatalf("effort max = %q", got)
	}
	if got := Normalize(map[string]any{"type": "disabled"}); got != "" {
		t.Fatalf("disabled = %q", got)
	}
	if got := Normalize(map[string]any{"type": "adaptive"}); got != ClientMedium {
		t.Fatalf("adaptive = %q want medium", got)
	}
}

func TestFromRequest(t *testing.T) {
	if got := FromRequest(map[string]any{"reasoning_effort": "auto"}); got != ClientLow {
		t.Fatalf("auto = %q", got)
	}
	if got := FromRequest(map[string]any{"reasoning_effort": "base"}); got != ClientMedium {
		t.Fatalf("codex base = %q want medium", got)
	}
	if got := FromRequest(map[string]any{"reasoning_effort": "proactive"}); got != ClientHigh {
		t.Fatalf("codex proactive = %q want high", got)
	}
	if got := FromRequest(map[string]any{"reasoning": map[string]any{"effort": "ultra"}}); got != ClientUltracode {
		t.Fatalf("codex ultra = %q want ultracode", got)
	}
	if got := FromRequest(map[string]any{"reasoning": map[string]any{"effort": "extra-high"}}); got != ClientXHigh {
		t.Fatalf("nested extra-high = %q want xhigh", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 9000}}); got != ClientHigh {
		t.Fatalf("thinking = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": "standard"}); got != ClientHigh {
		t.Fatalf("thinking string = %q", got)
	}
	if got := FromRequest(map[string]any{"thinking": map[string]any{"effort": "xhigh"}}); got != ClientXHigh {
		t.Fatalf("claude xhigh = %q want xhigh", got)
	}
	// Modern Anthropic / Claude Code: output_config.effort
	if got := FromRequest(map[string]any{"output_config": map[string]any{"effort": "max"}}); got != ClientMax {
		t.Fatalf("output_config max = %q", got)
	}
	if got := FromRequest(map[string]any{"output_config": map[string]any{"effort": "xhigh"}}); got != ClientXHigh {
		t.Fatalf("output_config xhigh = %q", got)
	}
	if got := FromRequest(map[string]any{"output_config": map[string]any{"effort": "ultracode"}}); got != ClientUltracode {
		t.Fatalf("output_config ultracode = %q", got)
	}
	if got := FromRequest(map[string]any{"effort": "ultracode"}); got != ClientUltracode {
		t.Fatalf("top-level ultracode = %q", got)
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
	// Client top tiers and aliases fold to Grok's literal xhigh.
	for _, label := range []string{"xhigh", "max", "ultracode", "extra-high", "Ultra"} {
		body = map[string]any{"reasoning_effort": label}
		if got := ApplyCanonical(body); got != XHigh || body["reasoning_effort"] != XHigh {
			t.Fatalf("%s must fold to xhigh, got %q body=%v", label, got, body)
		}
	}
	body = map[string]any{"reasoning_effort": "proactive"}
	if got := ApplyCanonical(body); got != High || body["reasoning_effort"] != High {
		t.Fatalf("proactive must fold to high, got %q body=%v", got, body)
	}
	// Codex Base → medium upstream.
	body = map[string]any{"reasoning_effort": "base"}
	if got := ApplyCanonical(body); got != Medium || body["reasoning_effort"] != Medium {
		t.Fatalf("base must fold to medium, got %q body=%v", got, body)
	}
	// output_config.effort must also fold when applying upstream.
	body = map[string]any{"output_config": map[string]any{"effort": "max"}}
	if got := ApplyCanonical(body); got != XHigh || body["reasoning_effort"] != XHigh {
		t.Fatalf("output_config max fold = %q body=%v", got, body)
	}
}

func TestFromRequestUpstream(t *testing.T) {
	if got := FromRequestUpstream(map[string]any{"output_config": map[string]any{"effort": "xhigh"}}); got != XHigh {
		t.Fatalf("upstream xhigh = %q", got)
	}
	if got := FromRequestUpstream(map[string]any{"effort": "ultracode"}); got != XHigh {
		t.Fatalf("upstream ultracode = %q", got)
	}
	if got := FromRequestUpstream(map[string]any{"effort": "max"}); got != XHigh {
		t.Fatalf("upstream max = %q", got)
	}
	if got := FromRequestUpstream(map[string]any{"reasoning_effort": "extra-high"}); got != XHigh {
		t.Fatalf("upstream extra-high = %q", got)
	}
	if got := FromRequestUpstream(map[string]any{"reasoning_effort": "low"}); got != Low {
		t.Fatalf("upstream low = %q", got)
	}
	if got := FromRequestUpstream(map[string]any{"reasoning_effort": "base"}); got != Medium {
		t.Fatalf("upstream base = %q want medium", got)
	}
	if got := FromRequestUpstream(map[string]any{"reasoning_effort": "proactive"}); got != High {
		t.Fatalf("upstream proactive = %q want high", got)
	}
	if got := FromRequestUpstream(map[string]any{"reasoning_effort": "Ultra"}); got != XHigh {
		t.Fatalf("upstream Ultra = %q want xhigh", got)
	}
}

func TestClientLabels(t *testing.T) {
	labels := ClientLabels()
	if len(labels) != 6 {
		t.Fatalf("want 6 labels, got %v", labels)
	}
	for _, want := range []string{"low", "medium", "high", "xhigh", "max", "ultracode"} {
		if !IsClientLabel(want) {
			t.Fatalf("IsClientLabel(%q)=false", want)
		}
	}
}
