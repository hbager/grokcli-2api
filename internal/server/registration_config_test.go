package server

import (
	"encoding/json"
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
