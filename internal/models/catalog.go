package models

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/provider"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
)

type Catalog struct {
	cfg      config.Config
	configFn func() config.Config
	store    *postgres.Connector
}

func NewCatalog(cfg config.Config, store *postgres.Connector) *Catalog {
	return &Catalog{cfg: cfg, store: store}
}

func NewDynamicCatalog(configFn func() config.Config, store *postgres.Connector) *Catalog {
	return &Catalog{configFn: configFn, store: store}
}

func (c *Catalog) OpenAIList(ctx context.Context) map[string]any {
	return map[string]any{"object": "list", "data": c.PublicModels(ctx)}
}

func (c *Catalog) PublicModels(ctx context.Context) []map[string]any {
	defaultModel := c.defaultModel()
	var models []map[string]any
	if c != nil && c.store != nil {
		rows, err := c.store.ListModels(ctx, false)
		if err == nil && len(rows) > 0 {
			models = make([]map[string]any, 0, len(rows)+16)
			now := time.Now().Unix()
			for _, row := range rows {
				if strings.TrimSpace(row.ID) == "" {
					continue
				}
				models = append(models, publicModelEntry(row, now))
			}
		}
	}
	if len(models) == 0 {
		models = fallbackModels(defaultModel)
	} else {
		models = mergeExtraModels(models, defaultModel)
	}
	// Always expose static web/console + tag bare build rows.
	models = provider.TagBuildProvider(models)
	models = provider.MergeStaticInto(models)
	sort.SliceStable(models, func(i, j int) bool {
		return modelSortKey(models[i], defaultModel) < modelSortKey(models[j], defaultModel)
	})
	return models
}

func (c *Catalog) Resolve(model string) string {
	defaultModel := c.defaultModel()
	m := strings.TrimSpace(model)
	if m == "" {
		return defaultModel
	}
	low := strings.ToLower(m)
	// Keep explicit multi-provider ids intact (web/... console/...).
	if strings.HasPrefix(low, "web/") || strings.HasPrefix(low, "console/") {
		return m
	}
	if low == "grok-search" || low == "web-search" {
		return defaultModel
	}
	if resolved, ok := aliases(defaultModel)[m]; ok {
		return resolved
	}
	if resolved, ok := aliases(defaultModel)[low]; ok {
		return resolved
	}
	return m
}

// ResolveRoute returns provider + upstream after alias normalization.
func (c *Catalog) ResolveRoute(model string) provider.Route {
	return provider.ResolveRoute(c.Resolve(model), c.defaultModel())
}

func (c *Catalog) defaultModel() string {
	if c == nil {
		return "grok-4.5"
	}
	cfg := c.cfg
	if c.configFn != nil {
		cfg = c.configFn()
	}
	if strings.TrimSpace(cfg.DefaultModel) == "" {
		return "grok-4.5"
	}
	return strings.TrimSpace(cfg.DefaultModel)
}

func publicModelEntry(row postgres.ModelRecord, now int64) map[string]any {
	entry := map[string]any{
		"id":       row.ID,
		"object":   "model",
		"created":  now,
		"owned_by": row.OwnedBy,
	}
	if row.Name != nil && *row.Name != "" {
		entry["name"] = *row.Name
	}
	if row.Description != nil && *row.Description != "" {
		entry["description"] = *row.Description
	}
	if row.ContextWindow != nil {
		entry["context_window"] = *row.ContextWindow
	}
	if row.SupportsReasoningEffort != nil {
		entry["supports_reasoning_effort"] = *row.SupportsReasoningEffort
	}
	for _, key := range []string{"max_completion_tokens", "reasoning_effort", "reasoning_efforts", "auto_compact_threshold_percent", "supported_in_api"} {
		if value, ok := row.Extra[key]; ok && value != nil {
			entry[key] = value
		}
	}
	return entry
}

func fallbackModels(defaultModel string) []map[string]any {
	now := time.Now().Unix()
	models := []map[string]any{{
		"id":       defaultModel,
		"object":   "model",
		"created":  now,
		"owned_by": "xai",
	}}
	models = mergeExtraModels(models, defaultModel)
	sort.SliceStable(models, func(i, j int) bool {
		return modelSortKey(models[i], defaultModel) < modelSortKey(models[j], defaultModel)
	})
	return models
}

func mergeExtraModels(models []map[string]any, defaultModel string) []map[string]any {
	have := make(map[string]bool, len(models)+2)
	for _, item := range models {
		if id, _ := item["id"].(string); id != "" {
			have[strings.ToLower(id)] = true
		}
	}
	now := time.Now().Unix()
	for _, extra := range []map[string]any{
		{"id": "grok-build", "name": "Grok Build", "description": "Grok coding / build model (cli-chat-proxy)", "owned_by": "xai"},
		{"id": "grok-search", "name": "Grok Search", "description": "Grok with web search enabled (local alias)", "owned_by": "xai"},
	} {
		id := extra["id"].(string)
		if have[strings.ToLower(id)] {
			continue
		}
		extra["object"] = "model"
		extra["created"] = now
		extra["synthetic"] = true
		extra["sort_order"] = sortOrderFor(id, defaultModel)
		models = append(models, extra)
		have[strings.ToLower(id)] = true
	}
	return models
}

func modelSortKey(item map[string]any, defaultModel string) string {
	id, _ := item["id"].(string)
	return string(rune('0'+sortOrderFor(id, defaultModel))) + ":" + id
}

func sortOrderFor(id, defaultModel string) int {
	switch {
	case id == defaultModel:
		return 0
	case id == "grok-build":
		return 1
	case id == "grok-search":
		return 2
	case strings.HasPrefix(id, "console/"):
		return 3
	case strings.HasPrefix(id, "web/"):
		return 4
	default:
		return 9
	}
}

func aliases(defaultModel string) map[string]string {
	return map[string]string{
		"gpt-4": defaultModel, "gpt-4o": defaultModel, "gpt-3.5-turbo": defaultModel, "gpt-4-turbo": defaultModel,
		"claude": defaultModel, "claude-3": defaultModel, "claude-3-5-sonnet": defaultModel, "claude-3-5-sonnet-20240620": defaultModel,
		"claude-3-5-sonnet-20241022": defaultModel, "claude-3-5-haiku": defaultModel, "claude-3-5-haiku-20241022": defaultModel,
		"claude-3-haiku": defaultModel, "claude-3-haiku-20240307": defaultModel, "claude-3-opus": defaultModel, "claude-3-opus-20240229": defaultModel,
		"claude-3-sonnet": defaultModel, "claude-3-sonnet-20240229": defaultModel, "claude-sonnet-4": defaultModel, "claude-sonnet-4-0": defaultModel,
		"claude-sonnet-4-20250514": defaultModel, "claude-sonnet-4-5": defaultModel, "claude-sonnet-4-5-20250929": defaultModel,
		"claude-opus-4": defaultModel, "claude-opus-4-0": defaultModel, "claude-opus-4-20250514": defaultModel, "claude-opus-4-5": defaultModel,
		"claude-haiku-4": defaultModel, "claude-haiku-4-5": defaultModel, "claude-haiku-4-5-20251001": defaultModel,
		"grok": defaultModel, "grok-latest": defaultModel, "grok-build": "grok-build", "grok-build-latest": "grok-build", "grok-4.5-build-free": "grok-build",
	}
}
