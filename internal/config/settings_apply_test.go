package config

import (
	"testing"
	"time"
)

func TestRuntimeConfigPublishesImmutableSnapshots(t *testing.T) {
	runtime := NewRuntimeConfig(Config{DefaultModel: "before", MaxFailoverAttempts: 4})
	before := runtime.Load()
	runtime.ApplyStoreSettings(map[string]any{
		"default_model":         "after",
		"max_failover_attempts": float64(2),
	})
	after := runtime.Load()
	if before.DefaultModel != "before" || before.MaxFailoverAttempts != 4 {
		t.Fatalf("old snapshot mutated: %#v", before)
	}
	if after.DefaultModel != "after" || after.MaxFailoverAttempts != 2 {
		t.Fatalf("new snapshot not published: %#v", after)
	}
}

func TestRuntimeConfigClearsDefaultModelToInitialValue(t *testing.T) {
	runtime := NewRuntimeConfig(Config{DefaultModel: "env-model"})
	runtime.ApplyStoreSettings(map[string]any{"default_model": "db-model"})
	runtime.ApplyStoreSettings(map[string]any{"default_model": "   "})
	if got := runtime.Load().DefaultModel; got != "env-model" {
		t.Fatalf("DefaultModel after clear=%q want env-model", got)
	}
}

func TestApplyStoreSettingsOverlaysLiveFields(t *testing.T) {
	cfg := Config{
		DefaultModel:        "env-model",
		SSEKeepalive:        4 * time.Second,
		OutboundMaxTools:    1,
		MaxFailoverAttempts: 6,
	}
	cfg.ApplyStoreSettings(map[string]any{
		"default_model":             "db-model",
		"sse_keepalive":             float64(12),
		"outbound_max_tools":        float64(3),
		"outbound_max_tools_openai": float64(5),
		"outbound_tool_gap_sec":     float64(0.2),
		"max_failover_attempts":     float64(2),
		// ignored / maintainer-only
		"token_maintain_enabled": true,
	})
	if cfg.DefaultModel != "db-model" {
		t.Fatalf("DefaultModel=%q", cfg.DefaultModel)
	}
	if cfg.SSEKeepalive != 12*time.Second {
		t.Fatalf("SSEKeepalive=%s", cfg.SSEKeepalive)
	}
	if cfg.OutboundMaxTools != 3 {
		t.Fatalf("OutboundMaxTools=%d", cfg.OutboundMaxTools)
	}
	if cfg.OutboundMaxToolsOpenAI != 5 {
		t.Fatalf("OutboundMaxToolsOpenAI=%d", cfg.OutboundMaxToolsOpenAI)
	}
	if cfg.OutboundToolGap != 200*time.Millisecond {
		t.Fatalf("OutboundToolGap=%s", cfg.OutboundToolGap)
	}
	if cfg.MaxFailoverAttempts != 2 {
		t.Fatalf("MaxFailoverAttempts=%d", cfg.MaxFailoverAttempts)
	}
}

func TestApplyStoreSettingsPublishesOutboundProxySnapshot(t *testing.T) {
	runtime := NewRuntimeConfig(Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://env-proxy:8080",
		OutboundProxyUsername:   "env-user",
		OutboundProxyPassword:   "env-password",
		OutboundProxyStrategy:   "sticky",
	})
	before := runtime.Load()
	runtime.ApplyStoreSettings(map[string]any{
		"outbound_proxy_config": map[string]any{
			"enabled":        false,
			"proxy":          "http://db-proxy:8080",
			"proxy_username": "db-user",
			"proxy_password": "db-password",
			"proxy_strategy": "random",
		},
	})
	after := runtime.Load()
	if !before.OutboundProxyEnabled || before.OutboundProxy != "http://env-proxy:8080" {
		t.Fatalf("old proxy snapshot mutated: %#v", before)
	}
	if !after.OutboundProxyConfigured || after.OutboundProxyEnabled ||
		after.OutboundProxy != "http://db-proxy:8080" ||
		after.OutboundProxyUsername != "db-user" ||
		after.OutboundProxyPassword != "db-password" ||
		after.OutboundProxyStrategy != "random" {
		t.Fatalf("proxy snapshot not applied: %#v", after)
	}

	runtime.ApplyStoreSettings(map[string]any{
		"outbound_proxy_config": map[string]any{
			"enabled":        true,
			"proxy":          "",
			"proxy_username": "",
			"proxy_password": "",
			"proxy_strategy": "invalid",
		},
	})
	cleared := runtime.Load()
	if !cleared.OutboundProxyEnabled || cleared.OutboundProxy != "" ||
		cleared.OutboundProxyUsername != "" || cleared.OutboundProxyPassword != "" ||
		cleared.OutboundProxyStrategy != "round_robin" {
		t.Fatalf("proxy clear not applied: %#v", cleared)
	}
}

func TestApplyStoreSettingsIgnoresInvalid(t *testing.T) {
	cfg := Config{DefaultModel: "keep", SSEKeepalive: 4 * time.Second, OutboundMaxTools: 1, MaxFailoverAttempts: 4}
	cfg.ApplyStoreSettings(map[string]any{
		"default_model":         "   ",
		"sse_keepalive":         float64(1),  // below min
		"outbound_max_tools":    float64(99), // above max
		"max_failover_attempts": float64(65),
	})
	if cfg.DefaultModel != "keep" {
		t.Fatalf("empty model should not overwrite: %q", cfg.DefaultModel)
	}
	if cfg.SSEKeepalive != 4*time.Second {
		t.Fatalf("invalid keepalive applied: %s", cfg.SSEKeepalive)
	}
	if cfg.OutboundMaxTools != 1 {
		t.Fatalf("invalid max tools applied: %d", cfg.OutboundMaxTools)
	}
	if cfg.MaxFailoverAttempts != 4 {
		t.Fatalf("invalid max failover attempts applied: %d", cfg.MaxFailoverAttempts)
	}
}
