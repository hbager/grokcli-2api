package accounts

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestCollectNormalizedEntriesJWTString(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-1","email":"a@x.ai","exp":4102444800}`))
	token := "aaa." + payload + ".sig"
	result := CollectNormalizedEntries(token)
	if !result.OK || len(result.Normalized) != 1 {
		t.Fatalf("result=%#v", result)
	}
	for id, ent := range result.Normalized {
		if id != "https://auth.x.ai::user-1" {
			t.Fatalf("id=%s", id)
		}
		if ent["email"] != "a@x.ai" || ent["key"] != token {
			t.Fatalf("entry=%#v", ent)
		}
	}
}

func TestCollectNormalizedEntriesAuthMap(t *testing.T) {
	raw := map[string]any{
		"https://auth.x.ai::u1": map[string]any{
			"key":   "tok-1",
			"email": "u1@x.ai",
		},
	}
	result := CollectNormalizedEntries(raw)
	if !result.OK || len(result.Normalized) != 1 {
		t.Fatalf("%#v", result)
	}
}

func TestCollectNormalizedEntriesExportWrapper(t *testing.T) {
	raw := map[string]any{
		"auth": map[string]any{
			"https://auth.x.ai::u2": map[string]any{"access_token": "tok-2", "email": "u2@x.ai"},
		},
		"count": 1,
	}
	b, _ := json.Marshal(raw)
	result := CollectNormalizedEntries(string(b))
	if !result.OK || len(result.Normalized) != 1 {
		t.Fatalf("%#v", result)
	}
}

func TestMergeDurableAccountFields(t *testing.T) {
	entry := map[string]any{"key": "new"}
	old := map[string]any{"sso": "cookie-1", "password": "p"}
	MergeDurableAccountFields(entry, old)
	if entry["sso"] != "cookie-1" || entry["password"] != "p" {
		t.Fatalf("%#v", entry)
	}
}

func TestGetCloudflareCookiesFromSessionCookies(t *testing.T) {
	entry := map[string]any{
		"sso": "sso-token",
		"session_cookies": map[string]any{
			"sso":          "sso-token",
			"sso-rw":       "sso-token",
			"cf_clearance": "cf-value-1",
			"__cf_bm":      "bm-value",
			"other":        "ignore-me",
		},
	}
	got := GetCloudflareCookies(entry)
	if got["cf_clearance"] != "cf-value-1" || got["__cf_bm"] != "bm-value" {
		t.Fatalf("got=%#v", got)
	}
	if _, ok := got["other"]; ok {
		t.Fatalf("should not include non-cf cookies: %#v", got)
	}
	if _, ok := got["sso"]; ok {
		t.Fatalf("sso is not a cloudflare cookie: %#v", got)
	}
}

func TestGetCloudflareCookiesFromDedicatedField(t *testing.T) {
	entry := map[string]any{
		"cloudflare_cookies": map[string]any{
			"cf_clearance": "x",
			"_cfuvid":      "y",
		},
		"session_cookies": map[string]any{"cf_clearance": "old"},
	}
	got := GetCloudflareCookies(entry)
	// dedicated field wins
	if got["cf_clearance"] != "x" || got["_cfuvid"] != "y" {
		t.Fatalf("got=%#v", got)
	}
}

func TestBuildSSOCookieHeaderMergesCF(t *testing.T) {
	h := BuildSSOCookieHeader("tok", map[string]string{
		"cf_clearance": "cf1",
		"__cf_bm":      "bm1",
	})
	if !strings.Contains(h, "sso=tok") || !strings.Contains(h, "sso-rw=tok") {
		t.Fatalf("missing sso: %q", h)
	}
	if !strings.Contains(h, "cf_clearance=cf1") || !strings.Contains(h, "__cf_bm=bm1") {
		t.Fatalf("missing cf: %q", h)
	}
}

func TestBuildSSOCookieHeaderSSOOnly(t *testing.T) {
	h := BuildSSOCookieHeader("tok", nil)
	if h != "sso=tok; sso-rw=tok" {
		t.Fatalf("got=%q", h)
	}
}
