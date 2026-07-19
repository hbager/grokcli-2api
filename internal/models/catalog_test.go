package models

import (
	"testing"

	"github.com/hm2899/grokcli-2api/internal/config"
)

func TestCatalogUsesLiveDefaultModel(t *testing.T) {
	runtime := config.NewRuntimeConfig(config.Config{DefaultModel: "before"})
	catalog := NewDynamicCatalog(runtime.Load, nil)
	if got := catalog.Resolve(""); got != "before" {
		t.Fatalf("Resolve before update=%q", got)
	}
	runtime.ApplyStoreSettings(map[string]any{"default_model": "after"})
	if got := catalog.Resolve(""); got != "after" {
		t.Fatalf("Resolve after update=%q", got)
	}
}

func TestCatalogLoadsOneConfigSnapshotPerOperation(t *testing.T) {
	loads := 0
	catalog := NewDynamicCatalog(func() config.Config {
		loads++
		return config.Config{DefaultModel: "grok-4.5"}
	}, nil)
	catalog.Resolve("custom-model")
	if loads != 1 {
		t.Fatalf("Resolve config loads=%d want 1", loads)
	}
	loads = 0
	catalog.PublicModels(t.Context())
	if loads != 1 {
		t.Fatalf("PublicModels config loads=%d want 1", loads)
	}
}

func TestFallbackModelsIncludePythonExtras(t *testing.T) {
	catalog := NewCatalog(config.Config{DefaultModel: "grok-4.5"}, nil)
	items := catalog.PublicModels(t.Context())
	ids := map[string]bool{}
	for _, item := range items {
		id, _ := item["id"].(string)
		ids[id] = true
	}
	for _, id := range []string{"grok-4.5", "grok-build", "grok-search"} {
		if !ids[id] {
			t.Fatalf("missing model %s in %#v", id, items)
		}
	}
}

func TestResolveAliases(t *testing.T) {
	catalog := NewCatalog(config.Config{DefaultModel: "grok-4.5"}, nil)
	for input, want := range map[string]string{
		"":                         "grok-4.5",
		"gpt-4o":                   "grok-4.5",
		"claude-sonnet-4-20250514": "grok-4.5",
		"web-search":               "grok-4.5",
		"grok-build-latest":        "grok-build",
		"custom-model":             "custom-model",
	} {
		if got := catalog.Resolve(input); got != want {
			t.Fatalf("Resolve(%q)=%q want %q", input, got, want)
		}
	}
}
