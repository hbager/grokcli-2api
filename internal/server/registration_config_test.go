package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestRedactRegistrationConfigRemovesSecrets(t *testing.T) {
	cfg := redactRegistrationConfig(map[string]any{
		"api_key":          "mail-secret",
		"yescaptcha_key":   "captcha-secret",
		"proxy_password":   "proxy-secret",
		"proxy_username":   "alice",
		"proxy":            "http://alice:embedded-secret@proxy.example:8080",
		"mail_provider":    "moemail",
		"captcha_provider": "yescaptcha",
	})
	for _, key := range []string{"api_key", "yescaptcha_key", "proxy_password"} {
		if cfg[key] != "" || cfg[key+"_set"] != true {
			t.Fatalf("%s not redacted: %#v", key, cfg)
		}
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"mail-secret", "captcha-secret", "proxy-secret", "embedded-secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret %q leaked in %s", secret, encoded)
		}
	}
}

func TestRedactProxyURLCoversSupportedCredentialFormats(t *testing.T) {
	for input, want := range map[string]string{
		"http://alice:secret@proxy.example:8080":   "http://alice:***@proxy.example:8080",
		"proxy.example:8080:alice:secret":          "http://alice:***@proxy.example:8080",
		"socks5://proxy.example:1080:alice:secret": "socks5://alice:***@proxy.example:1080",
		"alice:secret@proxy.example:8080":          "alice:***@proxy.example:8080",
		"http://proxy.example:8080":                "http://proxy.example:8080",
	} {
		user := ""
		if input == "http://proxy.example:8080" {
			user = "alice"
		}
		if got := redactProxyURL(input, user); got != want {
			t.Fatalf("redactProxyURL(%q)=%q want %q", input, got, want)
		}
	}
}

func TestRedactAdminSettingsRemovesNestedSecrets(t *testing.T) {
	settings := redactAdminSettings(map[string]any{
		"registration_config": map[string]any{"api_key": "mail-secret"},
		"outbound_proxy_config": map[string]any{
			"proxy":          "http://alice:embedded-secret@proxy.example:8080",
			"proxy_password": "proxy-secret",
		},
		"sub2api_config":     map[string]any{"password": "sub-secret", "token": "sub-token"},
		"cliproxyapi_config": map[string]any{"management_key": "management-secret"},
	})
	encoded, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"mail-secret", "embedded-secret", "proxy-secret", "sub-secret", "sub-token", "management-secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret %q leaked in %s", secret, encoded)
		}
	}
}

func TestMergeRegistrationConfigPatchPreservesMaskedProxy(t *testing.T) {
	current := map[string]any{
		"proxy":          "http://alice:proxy-secret@proxy.example:8080",
		"proxy_username": "alice",
	}
	masked := stringValue(redactRegistrationConfig(current)["proxy"])
	merged, err := mergeRegistrationConfigPatch(current, map[string]any{"proxy": masked})
	if err != nil {
		t.Fatal(err)
	}
	if merged["proxy"] != current["proxy"] {
		t.Fatalf("masked proxy overwrote secret: %#v", merged)
	}
	if _, err := mergeRegistrationConfigPatch(current, map[string]any{"proxy": masked + "\nhttp://other:***@proxy2.example:8080"}); err == nil {
		t.Fatal("edited masked proxy should require full credentials")
	}
	cleared, err := mergeRegistrationConfigPatch(current, map[string]any{"proxy": ""})
	if err != nil {
		t.Fatal(err)
	}
	if cleared["proxy"] != "" {
		t.Fatalf("proxy not cleared: %#v", cleared)
	}
}

func TestMergeRegistrationConfigPatchClearsSecretOnlyWithExplicitFlag(t *testing.T) {
	current := map[string]any{
		"mail_provider":   "moemail",
		"api_key":         "mail-secret",
		"moemail_api_key": "mail-secret",
		"yescaptcha_key":  "captcha-secret",
	}

	preserved, err := mergeRegistrationConfigPatch(current, map[string]any{"api_key": ""})
	if err != nil {
		t.Fatal(err)
	}
	if preserved["api_key"] != "mail-secret" {
		t.Fatalf("blank placeholder should preserve secret: %#v", preserved)
	}

	cleared, err := mergeRegistrationConfigPatch(current, map[string]any{
		"api_key":              "",
		"api_key_clear":        true,
		"yescaptcha_key_clear": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cleared["api_key"] != "" || cleared["moemail_api_key"] != "" {
		t.Fatalf("explicit clear did not clear active provider secret: %#v", cleared)
	}
	if cleared["yescaptcha_key"] != "captcha-secret" {
		t.Fatalf("false clear flag changed secret: %#v", cleared)
	}
	if _, ok := cleared["api_key_clear"]; ok {
		t.Fatalf("control flag persisted: %#v", cleared)
	}

	startBody := cloneMapAny(current)
	applyRegistrationSecretClearFlags(startBody, map[string]any{"api_key_clear": true})
	if startBody["api_key"] != "" || startBody["api_key_clear"] != true {
		t.Fatalf("start body did not carry clear intent to auto-save: %#v", startBody)
	}
}

func TestMergeOutboundProxyPatchPreservesExactMaskAndRejectsEditedMask(t *testing.T) {
	current := map[string]any{
		"proxy":          "http://alice:proxy-secret@proxy.example:8080",
		"proxy_username": "alice",
	}
	masked := stringValue(redactAdminSettings(map[string]any{
		"outbound_proxy_config": current,
	})["outbound_proxy_config"].(map[string]any)["proxy"])

	preserved, err := mergeOutboundProxyPatch(current, map[string]any{"outbound_proxy": masked})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := preserved["outbound_proxy"]; ok {
		t.Fatalf("exact mask should be omitted from store patch: %#v", preserved)
	}

	if _, err := mergeOutboundProxyPatch(current, map[string]any{
		"outbound_proxy": masked + "\nhttp://other:***@proxy2.example:8080",
	}); err == nil {
		t.Fatal("edited outbound mask should require full credentials")
	}

	nested, err := mergeOutboundProxyPatch(current, map[string]any{
		"outbound_proxy_config": map[string]any{
			"proxy":              masked,
			"proxy_password":     "",
			"proxy_password_set": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	nestedConfig := nested["outbound_proxy_config"].(map[string]any)
	if _, ok := nestedConfig["proxy"]; ok {
		t.Fatalf("nested exact mask should be omitted: %#v", nestedConfig)
	}
	if _, ok := nestedConfig["proxy_password"]; ok {
		t.Fatalf("blank nested password placeholder should be omitted: %#v", nestedConfig)
	}
	if _, ok := nestedConfig["proxy_password_set"]; ok {
		t.Fatalf("nested response metadata should be omitted: %#v", nestedConfig)
	}

	replaced, err := mergeOutboundProxyPatch(current, map[string]any{
		"outbound_proxy": "http://alice:new-secret@proxy.example:8080",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replaced["outbound_proxy"] != "http://alice:new-secret@proxy.example:8080" {
		t.Fatalf("full replacement changed: %#v", replaced)
	}
}

func TestMergeOutboundProxyPatchClearsPasswordWithExplicitFlag(t *testing.T) {
	current := map[string]any{"proxy_password": "shared-secret"}
	cases := map[string]map[string]any{
		"flat":   {"outbound_proxy_password_clear": true},
		"nested": {"outbound_proxy_config": map[string]any{"proxy_password": "", "proxy_password_clear": true}},
	}
	for name, patch := range cases {
		t.Run(name, func(t *testing.T) {
			merged, err := mergeOutboundProxyPatch(current, patch)
			if err != nil {
				t.Fatal(err)
			}
			config, ok := merged["outbound_proxy_config"].(map[string]any)
			if !ok {
				t.Fatalf("clear did not produce outbound proxy config: %#v", merged)
			}
			if password, exists := config["proxy_password"]; !exists || password != "" {
				t.Fatalf("password not explicitly cleared: %#v", config)
			}
			if _, exists := config["proxy_password_clear"]; exists {
				t.Fatalf("clear control persisted: %#v", config)
			}
			if _, exists := merged["outbound_proxy_password_clear"]; exists {
				t.Fatalf("flat clear control persisted: %#v", merged)
			}
		})
	}
}

func TestNormalizeRegistrationConfigDefaults(t *testing.T) {
	cfg := normalizeRegistrationConfig(map[string]any{})
	if cfg["mail_provider"] != "moemail" {
		t.Fatalf("mail_provider=%v", cfg["mail_provider"])
	}
	if cfg["captcha_provider"] != "local" {
		t.Fatalf("captcha_provider=%v", cfg["captcha_provider"])
	}
	if cfg["local_solver_url"] != "http://127.0.0.1:5072" {
		t.Fatalf("local_solver_url=%v", cfg["local_solver_url"])
	}
	if cfg["proxy_strategy"] != "round_robin" {
		t.Fatalf("proxy_strategy=%v", cfg["proxy_strategy"])
	}
}

func TestIsMaskedSecret(t *testing.T) {
	if !isMaskedSecret("ab…cd") || !isMaskedSecret("****") {
		t.Fatal("expected masked")
	}
	if isMaskedSecret("real-secret-key") {
		t.Fatal("plain secret should not be masked")
	}
}

func TestSplitProxyLines(t *testing.T) {
	lines := splitProxyLines("http://a:1\n#c\nhttp://b:2;http://c:3")
	if len(lines) != 3 {
		t.Fatalf("lines=%v", lines)
	}
}

func TestMailSecretFitsSlot(t *testing.T) {
	tests := []struct {
		name string
		slot string
		key  string
		fits bool
	}{
		{name: "MoeMail", slot: "moemail_api_key", key: "mk_abc", fits: true},
		{name: "MoeMail rejects YYDS", slot: "moemail_api_key", key: "AC-yyds"},
		{name: "MoeMail rejects GPTMail", slot: "moemail_api_key", key: "sk-gpt"},
		{name: "YYDS", slot: "yyds_api_key", key: "AC-yyds", fits: true},
		{name: "YYDS rejects MoeMail", slot: "yyds_api_key", key: "mk_abc"},
		{name: "GPTMail", slot: "gptmail_api_key", key: "sk-gpt", fits: true},
		{name: "GPTMail rejects YYDS", slot: "gptmail_api_key", key: "AC-yyds"},
		{name: "GPTMail rejects MoeMail", slot: "gptmail_api_key", key: "mk_abc"},
		{name: "CFMail", slot: "cfmail_api_key", key: "cf-token", fits: true},
		{name: "TempMail free", slot: "tempmail_api_key", key: "", fits: true},
		{name: "TempMail paid", slot: "tempmail_api_key", key: "some-paid-bearer-token", fits: true},
		{name: "TempMail rejects MoeMail", slot: "tempmail_api_key", key: "mk_moe"},
		{name: "TempMail rejects YYDS", slot: "tempmail_api_key", key: "AC-yyds"},
		{name: "CloudMail", slot: "cloudmail_api_key", key: "00000000-1111-2222-3333-444444444444", fits: true},
		{name: "CloudMail rejects MoeMail", slot: "cloudmail_api_key", key: "mk_moe"},
		{name: "CloudMail rejects YYDS", slot: "cloudmail_api_key", key: "AC-yyds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mailSecretFitsSlot(tt.slot, tt.key); got != tt.fits {
				t.Fatalf("mailSecretFitsSlot(%q, %q)=%v want %v", tt.slot, tt.key, got, tt.fits)
			}
		})
	}
}

func TestSanitizeRegistrationMailSecretsMovesPollutedKeys(t *testing.T) {
	cfg := map[string]any{
		"mail_provider":    "moemail",
		"moemail_api_key":  "AC-c1965a37122be549cc25724a",
		"yyds_api_key":     "",
		"gptmail_api_key":  "AC-c1965a37122be549cc25724a",
		"moemail_domain":   "lolicr.com",
		"moemail_base_url": "",
		"domain":           "lolicr.com",
		"api_key":          "AC-c1965a37122be549cc25724a",
	}
	sanitizeRegistrationMailSecrets(cfg)
	if cfg["moemail_api_key"] != "" {
		t.Fatalf("polluted moemail key should be cleared, got %v", cfg["moemail_api_key"])
	}
	if cfg["yyds_api_key"] != "AC-c1965a37122be549cc25724a" {
		t.Fatalf("AC- should move to yyds, got %v", cfg["yyds_api_key"])
	}
	if cfg["gptmail_api_key"] != "" {
		t.Fatalf("polluted gptmail key should be cleared, got %v", cfg["gptmail_api_key"])
	}
	if cfg["api_key"] != "" {
		t.Fatalf("moemail active api_key should be empty after sanitize, got %v", cfg["api_key"])
	}
}

func TestRegistrationConfigPatchForPersistDropsAdapterRemap(t *testing.T) {
	req := map[string]any{
		"mail_provider":    "yyds",
		"yyds_api_key":     "AC-new-yyds",
		"moemail_api_key":  "mk_real_moemail",
		"moemail_base_url": "https://moemail.example.com",
		"domain":           "",
	}
	merged := map[string]any{
		"mail_provider":    "yyds",
		"yyds_api_key":     "AC-new-yyds",
		"moemail_api_key":  "AC-new-yyds",
		"moemail_base_url": "",
		"base_url":         "",
		"api_key":          "AC-new-yyds",
		"domain":           "",
	}
	patch := registrationConfigPatchForPersist(req, merged)
	if _, ok := patch["moemail_api_key"]; ok {
		t.Fatalf("patch must not include remapped moemail_api_key, got %v", patch["moemail_api_key"])
	}
	if patch["yyds_api_key"] != "AC-new-yyds" {
		t.Fatalf("yyds key missing, got %v", patch["yyds_api_key"])
	}
	if patch["moemail_base_url"] != "https://moemail.example.com" {
		t.Fatalf("durable moemail_base_url should come from request, got %v", patch["moemail_base_url"])
	}

	req = map[string]any{"mail_provider": "yyds", "yyds_api_key": "AC-x"}
	merged = map[string]any{
		"mail_provider": "yyds", "yyds_api_key": "AC-x",
		"moemail_api_key": "AC-x", "moemail_base_url": "", "base_url": "",
	}
	patch = registrationConfigPatchForPersist(req, merged)
	if _, ok := patch["moemail_api_key"]; ok {
		t.Fatal("patch must not include remapped moemail_api_key")
	}
	if v, ok := patch["moemail_base_url"]; ok && strings.TrimSpace(fmt.Sprint(v)) == "" {
		t.Fatal("empty remapped moemail_base_url should be dropped from patch")
	}
}

func TestMergeRegistrationStartBodyProviderIsolation(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		body     map[string]any
		wantKey  string
		wantURL  string
	}{
		{name: "MoeMail", provider: "moemail", body: map[string]any{
			"moemail_api_key":  "mk_moe",
			"moemail_base_url": "https://moemail.example.com",
		}, wantKey: "mk_moe", wantURL: "https://moemail.example.com"},
		{name: "YYDS", provider: "yyds", body: map[string]any{"yyds_api_key": "AC-yyds"}, wantKey: "AC-yyds"},
		{name: "GPTMail", provider: "gptmail", body: map[string]any{"gptmail_api_key": "sk-gpt"}, wantKey: "sk-gpt"},
		{name: "CFMail", provider: "cfmail", body: map[string]any{
			"cfmail_api_key":  "cf-token",
			"cfmail_base_url": "https://cfmail.example.com",
		}, wantKey: "cf-token", wantURL: "https://cfmail.example.com"},
		{name: "TempMail", provider: "tempmail", body: map[string]any{"tempmail_api_key": "paid-token"}, wantKey: "paid-token"},
		{name: "CloudMail", provider: "cloudmail", body: map[string]any{
			"cloudmail_api_key":  "cm-token",
			"cloudmail_base_url": "https://cmail.example.com",
		}, wantKey: "cm-token", wantURL: "https://cmail.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := map[string]any{
				"moemail_api_key":  "mk_saved_moe",
				"moemail_base_url": "https://saved-moe.example.com",
			}
			for key, value := range tt.body {
				body[key] = value
			}
			body["mail_provider"] = tt.provider
			out := mergeRegistrationStartBody(context.Background(), Options{}, body)
			if out["mail_provider"] != tt.provider {
				t.Fatalf("mail_provider=%v", out["mail_provider"])
			}
			if out["moemail_api_key"] != tt.wantKey || out["api_key"] != tt.wantKey {
				t.Fatalf("active key not isolated: %#v", out)
			}
			if got := stringValue(out["moemail_base_url"]); got != tt.wantURL {
				t.Fatalf("active base URL=%q want %q", got, tt.wantURL)
			}
		})
	}
}

func TestNormalizeRegistrationConfigMailAliases(t *testing.T) {
	tests := map[string]string{
		"yydsmail":     "yyds",
		"chatgptmail":  "gptmail",
		"cloudflare":   "cfmail",
		"tempmail.lol": "tempmail",
		"lol":          "tempmail",
		"skymail":      "cloudmail",
		"cmail":        "cloudmail",
	}
	for alias, want := range tests {
		t.Run(alias, func(t *testing.T) {
			cfg := normalizeRegistrationConfig(map[string]any{"mail_provider": alias})
			if cfg["mail_provider"] != want {
				t.Fatalf("mail_provider=%v want %s", cfg["mail_provider"], want)
			}
		})
	}
}

func TestSanitizeTempmailEmptyKeyOK(t *testing.T) {
	cfg := map[string]any{
		"mail_provider":    "tempmail",
		"tempmail_api_key": "",
		"tempmail_domain":  "",
		"moemail_api_key":  "mk_should_not_leak",
		"api_key":          "mk_should_not_leak",
	}
	sanitizeRegistrationMailSecrets(cfg)
	if cfg["tempmail_api_key"] != "" {
		t.Fatalf("tempmail key should stay empty, got %v", cfg["tempmail_api_key"])
	}
	if cfg["api_key"] != "" {
		t.Fatalf("tempmail active api_key should be empty free tier, got %v", cfg["api_key"])
	}
}

func TestRegistrationConfigPatchForPersistClearsTempmailKey(t *testing.T) {
	req := map[string]any{
		"mail_provider":    "tempmail",
		"tempmail_api_key": "",
		"tempmail_domain":  "",
	}
	merged := map[string]any{
		"mail_provider":    "tempmail",
		"tempmail_api_key": "old-paid-key",
		"tempmail_domain":  "custom.example",
		"moemail_api_key":  "mk_other",
		"moemail_domain":   "lolicr.com",
	}
	patch := registrationConfigPatchForPersist(req, merged)
	if v := strings.TrimSpace(fmt.Sprint(patch["tempmail_api_key"])); v != "" && v != "<nil>" {
		t.Fatalf("expected cleared tempmail key, got %v", patch["tempmail_api_key"])
	}
	if v := strings.TrimSpace(fmt.Sprint(patch["tempmail_domain"])); v != "" && v != "<nil>" {
		t.Fatalf("expected cleared tempmail domain, got %v", patch["tempmail_domain"])
	}
	if v, ok := patch["moemail_api_key"]; ok {
		if s := strings.TrimSpace(fmt.Sprint(v)); s != "" && s != "mk_other" {
			t.Fatalf("moemail key remapped unexpectedly: %v", v)
		}
	}
}
