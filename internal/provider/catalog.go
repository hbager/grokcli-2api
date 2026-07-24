// Package provider holds multi-channel model catalogs (build / web / console).
// Phase 1: static web+console lists + route resolution. Upstream clients come later.
package provider

import "strings"

type Kind string

const (
	Build   Kind = "build"
	Web     Kind = "web"
	Console Kind = "console"
)

const (
	AuthToken = "token"
	AuthSSO   = "sso"
)

type Route struct {
	PublicID string
	Provider Kind
	Upstream string
	Auth     string
	// ReasoningEffort forced on upstream body when non-empty.
	ReasoningEffort string
}

type Spec struct {
	LocalID     string
	Upstream    string
	Name        string
	Description string
	Capability  string
	// ReasoningEffort if set is forced on console/web outbound (e.g. multi-agent xhigh → 16 agents).
	ReasoningEffort string
}

var WebCatalog = []Spec{
	{LocalID: "grok-chat-fast", Upstream: "grok-chat-fast", Name: "Grok Chat Fast", Capability: "chat"},
	{LocalID: "grok-chat-auto", Upstream: "grok-chat-auto", Name: "Grok Chat Auto", Capability: "chat"},
	{LocalID: "grok-chat-expert", Upstream: "grok-chat-expert", Name: "Grok Chat Expert", Capability: "chat"},
	{LocalID: "grok-chat-heavy", Upstream: "grok-chat-heavy", Name: "Grok Chat Heavy", Capability: "chat"},
	{LocalID: "grok-imagine-image", Upstream: "grok-imagine-image", Name: "Grok Imagine Image", Capability: "image"},
	{LocalID: "grok-imagine-image-quality", Upstream: "grok-imagine-image-quality", Name: "Grok Imagine Image Quality", Capability: "image"},
	{LocalID: "grok-imagine-image-edit", Upstream: "imagine-image-edit", Name: "Grok Imagine Image Edit", Capability: "image_edit"},
	{LocalID: "grok-imagine-video", Upstream: "grok-imagine-video", Name: "Grok Imagine Video", Capability: "video"},
}

var ConsoleCatalog = []Spec{
	{LocalID: "grok-4.3", Upstream: "grok-4.3", Name: "Grok 4.3", Capability: "responses"},
	{LocalID: "grok-4.20-0309", Upstream: "grok-4.20-0309", Name: "Grok 4.20 0309", Capability: "responses"},
	{LocalID: "grok-4.20-0309-reasoning", Upstream: "grok-4.20-0309-reasoning", Name: "Grok 4.20 Reasoning", Capability: "responses"},
	{LocalID: "grok-4.20-0309-non-reasoning", Upstream: "grok-4.20-0309-non-reasoning", Name: "Grok 4.20 Non-Reasoning", Capability: "responses"},
	{LocalID: "grok-4.20-multi-agent-0309", Upstream: "grok-4.20-multi-agent-0309", Name: "Grok 4.20 Multi-Agent", Capability: "responses"},
	// Docs: grok-4.20-multi-agent + reasoning.effort xhigh/high → 16 agents.
	// https://docs.x.ai/developers/model-capabilities/text/multi-agent
	{LocalID: "grok-4.20-multi-agent-xhigh", Upstream: "grok-4.20-multi-agent", Name: "Grok 4.20 Multi-Agent 16 (xhigh)", Description: "Realtime multi-agent research with 16 agents (reasoning.effort=xhigh)", Capability: "responses", ReasoningEffort: "xhigh"},
	{LocalID: "grok-build-0.1", Upstream: "grok-build-0.1", Name: "Grok Build 0.1 (Console)", Capability: "responses"},
}

func PublicID(kind Kind, local string) string {
	local = strings.TrimSpace(local)
	if local == "" {
		return ""
	}
	switch kind {
	case Web:
		return "web/" + local
	case Console:
		return "console/" + local
	default:
		return local
	}
}

func StaticEntries() []map[string]any {
	out := make([]map[string]any, 0, len(WebCatalog)+len(ConsoleCatalog))
	for _, spec := range WebCatalog {
		out = append(out, staticEntry(Web, spec))
	}
	for _, spec := range ConsoleCatalog {
		out = append(out, staticEntry(Console, spec))
	}
	return out
}

func staticEntry(kind Kind, spec Spec) map[string]any {
	id := PublicID(kind, spec.LocalID)
	name := spec.Name
	if name == "" {
		name = id
	}
	entry := map[string]any{
		"id":          id,
		"name":        name,
		"description": spec.Description,
		"owned_by":    "xai",
		"provider":    string(kind),
		"upstream":    firstNonEmpty(spec.Upstream, spec.LocalID),
		"capability":  spec.Capability,
		"auth":        AuthSSO,
		"synthetic":   true,
	}
	if e := strings.TrimSpace(spec.ReasoningEffort); e != "" {
		entry["reasoning_effort"] = e
		entry["fixed_reasoning_effort"] = e
	}
	return entry
}

func ResolveRoute(model, defaultBuild string) Route {
	m := strings.TrimSpace(model)
	if m == "" {
		m = strings.TrimSpace(defaultBuild)
		if m == "" {
			m = "grok-4.5"
		}
	}
	low := strings.ToLower(m)
	if strings.HasPrefix(low, "web/") {
		local := strings.TrimSpace(m[len("web/"):])
		if spec, ok := findSpec(WebCatalog, local); ok {
			return routeFromSpec(Web, spec)
		}
		return Route{PublicID: PublicID(Web, local), Provider: Web, Upstream: local, Auth: AuthSSO}
	}
	if strings.HasPrefix(low, "console/") {
		local := strings.TrimSpace(m[len("console/"):])
		if spec, ok := findSpec(ConsoleCatalog, local); ok {
			return routeFromSpec(Console, spec)
		}
		return Route{PublicID: PublicID(Console, local), Provider: Console, Upstream: local, Auth: AuthSSO}
	}
	if isConsoleLocalID(m) {
		if spec, ok := findSpec(ConsoleCatalog, m); ok {
			return routeFromSpec(Console, spec)
		}
	}
	return Route{PublicID: m, Provider: Build, Upstream: m, Auth: AuthToken}
}

func routeFromSpec(kind Kind, spec Spec) Route {
	return Route{
		PublicID:        PublicID(kind, spec.LocalID),
		Provider:        kind,
		Upstream:        firstNonEmpty(spec.Upstream, spec.LocalID),
		Auth:            AuthSSO,
		ReasoningEffort: strings.TrimSpace(spec.ReasoningEffort),
	}
}

func isConsoleLocalID(id string) bool {
	switch strings.ToLower(strings.TrimSpace(id)) {
	case "grok-4.3", "grok-4.20-0309", "grok-4.20-0309-reasoning",
		"grok-4.20-0309-non-reasoning", "grok-4.20-multi-agent-0309",
		"grok-4.20-multi-agent", "grok-4.20-multi-agent-xhigh", "grok-build-0.1":
		return true
	default:
		return false
	}
}

func findSpec(list []Spec, local string) (Spec, bool) {
	local = strings.TrimSpace(local)
	for _, spec := range list {
		if strings.EqualFold(spec.LocalID, local) || strings.EqualFold(spec.Upstream, local) {
			return spec, true
		}
	}
	return Spec{}, false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func MergeStaticInto(items []map[string]any) []map[string]any {
	have := make(map[string]bool, len(items)+16)
	for _, it := range items {
		if id, _ := it["id"].(string); id != "" {
			have[strings.ToLower(strings.TrimSpace(id))] = true
		}
	}
	for _, entry := range StaticEntries() {
		id, _ := entry["id"].(string)
		if id == "" || have[strings.ToLower(id)] {
			continue
		}
		items = append(items, entry)
		have[strings.ToLower(id)] = true
	}
	return items
}

func TagBuildProvider(items []map[string]any) []map[string]any {
	for _, it := range items {
		if it == nil {
			continue
		}
		if p, _ := it["provider"].(string); strings.TrimSpace(p) != "" {
			continue
		}
		id, _ := it["id"].(string)
		low := strings.ToLower(strings.TrimSpace(id))
		if strings.HasPrefix(low, "web/") || strings.HasPrefix(low, "console/") {
			continue
		}
		it["provider"] = string(Build)
		if _, ok := it["auth"]; !ok {
			it["auth"] = AuthToken
		}
	}
	return items
}
