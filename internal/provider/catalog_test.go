package provider

import "testing"

func TestStaticEntriesHaveProviderPrefix(t *testing.T) {
	items := StaticEntries()
	if len(items) < 10 {
		t.Fatalf("static entries too few: %d", len(items))
	}
	haveWeb, haveConsole := false, false
	for _, it := range items {
		id, _ := it["id"].(string)
		p, _ := it["provider"].(string)
		if id == "web/grok-chat-fast" && p == "web" {
			haveWeb = true
		}
		if id == "console/grok-4.3" && p == "console" {
			haveConsole = true
		}
		if it["synthetic"] != true {
			t.Fatalf("%s must be synthetic so ReplaceModels keeps them", id)
		}
		if it["auth"] != AuthSSO {
			t.Fatalf("%s auth=%v want sso", id, it["auth"])
		}
	}
	if !haveWeb || !haveConsole {
		t.Fatalf("missing web/console entries: %#v", items)
	}
}

func TestResolveRoute(t *testing.T) {
	cases := []struct {
		in       string
		provider Kind
		upstream string
		auth     string
	}{
		{"", Build, "grok-4.5", AuthToken},
		{"grok-4.5", Build, "grok-4.5", AuthToken},
		{"web/grok-chat-fast", Web, "grok-chat-fast", AuthSSO},
		{"console/grok-4.3", Console, "grok-4.3", AuthSSO},
		{"grok-4.20-0309", Console, "grok-4.20-0309", AuthSSO},
		{"custom-build", Build, "custom-build", AuthToken},
	}
	for _, tc := range cases {
		r := ResolveRoute(tc.in, "grok-4.5")
		if r.Provider != tc.provider || r.Upstream != tc.upstream || r.Auth != tc.auth {
			t.Fatalf("ResolveRoute(%q)=%+v want provider=%s upstream=%s auth=%s", tc.in, r, tc.provider, tc.upstream, tc.auth)
		}
	}
}

func TestMergeStaticInto(t *testing.T) {
	base := []map[string]any{{"id": "grok-4.5", "owned_by": "xai"}}
	out := MergeStaticInto(base)
	ids := map[string]bool{}
	for _, it := range out {
		id, _ := it["id"].(string)
		ids[id] = true
	}
	if !ids["grok-4.5"] || !ids["web/grok-chat-fast"] || !ids["console/grok-4.3"] {
		t.Fatalf("merge missing entries: %#v", ids)
	}
	out2 := MergeStaticInto(out)
	if len(out2) != len(out) {
		t.Fatalf("merge not idempotent: %d -> %d", len(out), len(out2))
	}
}

func TestResolveMultiAgentXHigh(t *testing.T) {
	r := ResolveRoute("console/grok-4.20-multi-agent-xhigh", "grok-4.5")
	if r.Provider != Console || r.Upstream != "grok-4.20-multi-agent" || r.ReasoningEffort != "xhigh" {
		t.Fatalf("route=%+v", r)
	}
	if r.PublicID != "console/grok-4.20-multi-agent-xhigh" {
		t.Fatalf("public=%q", r.PublicID)
	}
}
