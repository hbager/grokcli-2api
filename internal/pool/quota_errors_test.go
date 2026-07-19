package pool

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClassifyFreeUsageEnglish(t *testing.T) {
	body := `{"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for model grok-4.5-build-free for now. Usage resets over a rolling 24-hour window — tokens (actual/limit): 2042591/2000000."}`
	d := ClassifyUpstreamFailure(429, body)
	if !d.ShouldCooldown || d.Class != ClassFreeUsage {
		t.Fatalf("decision=%+v", d)
	}
	if d.Model != "grok-4.5" {
		t.Fatalf("model=%q", d.Model)
	}
	if d.BlockModel {
		t.Fatal("free-usage must cool only, not soft-block model")
	}
	if d.Until == nil || d.Until.Before(time.Now().Add(90*time.Minute)) {
		t.Fatalf("until=%v", d.Until)
	}
	if d.TokensActual == nil || *d.TokensActual != 2042591 || d.TokensLimit == nil || *d.TokensLimit != 2000000 {
		t.Fatalf("tokens a=%v b=%v", d.TokensActual, d.TokensLimit)
	}
}

func TestClassifyFreeUsageChinese(t *testing.T) {
	d := ClassifyUpstreamFailure(429, "账号免费额度耗尽，请稍后再试")
	if d.Class != ClassFreeUsage || !d.ShouldCooldown {
		t.Fatalf("%+v", d)
	}
}

func TestClassifyRateLimitWithoutQuota(t *testing.T) {
	d := ClassifyUpstreamFailure(429, "rate limit exceeded, try again later")
	if d.Class != ClassRateLimit {
		t.Fatalf("%+v", d)
	}
	if d.Until == nil || d.Until.After(time.Now().Add(15*time.Minute)) {
		t.Fatalf("rate limit cool too long: %v", d.Until)
	}
}

func TestClassifyBadRequestNoCooldown(t *testing.T) {
	d := ClassifyUpstreamFailure(400, "invalid_request_error: messages required")
	if d.ShouldCooldown {
		t.Fatalf("must not cool: %+v", d)
	}
}

func TestClassifyAuth(t *testing.T) {
	d := ClassifyUpstreamFailure(401, "Invalid or missing API key")
	if d.Class != ClassAuth || !d.ShouldCooldown {
		t.Fatalf("%+v", d)
	}
}

func TestIsFreeUsageExhaustedHelper(t *testing.T) {
	if !IsFreeUsageExhausted("subscription:free-usage-exhausted") {
		t.Fatal("expected true")
	}
	if IsFreeUsageExhausted("hello world") {
		t.Fatal("expected false")
	}
}

func TestExtractModelAndTokens(t *testing.T) {
	if got := ExtractModelName("for model grok-4.5-build-free for now"); got != "grok-4.5" {
		t.Fatalf("model=%q", got)
	}
	a, b, ok := ParseTokenPair("tokens (actual/limit): 12/34")
	if !ok || a != 12 || b != 34 {
		t.Fatalf("%v %v %v", a, b, ok)
	}
}

func TestBillingVsFree(t *testing.T) {
	d := ClassifyUpstreamFailure(402, "insufficient_quota: billing hard limit")
	if d.Class != ClassBilling {
		t.Fatalf("%+v", d)
	}
}

func TestServerError(t *testing.T) {
	d := ClassifyUpstreamFailure(503, "upstream unavailable")
	if d.Class != ClassServer || !d.ShouldCooldown {
		t.Fatalf("%+v", d)
	}
	if d.Until != nil && d.Until.After(time.Now().Add(10*time.Minute)) {
		t.Fatalf("5xx cool too long")
	}
}

func TestCodeOnlyJSON(t *testing.T) {
	d := ClassifyUpstreamFailure(http.StatusTooManyRequests, `{"code":"subscription:free-usage-exhausted"}`)
	if d.Class != ClassFreeUsage {
		t.Fatalf("%+v", d)
	}
	if !strings.Contains(d.Code, "free-usage") {
		t.Fatalf("code=%q", d.Code)
	}
}

func TestClassifyQuotaTextEntersCooldownPool(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		class  FailureClass
	}{
		{"chinese_quota_used_up", 429, "模型额度用完，请稍后再试", ClassFreeUsage},
		{"chinese_no_quota", 0, "账号没额度了", ClassFreeUsage},
		{"english_out_of_quota", 429, "out of quota for this model", ClassFreeUsage},
		{"wrapped_upstream_error", 0, `upstream status 429: {"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for model grok-4.5-build-free for now. tokens (actual/limit): 10/10."}`, ClassFreeUsage},
		{"rate_limit_no_quota", 429, "rate limit exceeded, try again later", ClassRateLimit},
		{"validation_no_cool", 400, "invalid_request_error: messages required", ClassNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := ClassifyUpstreamFailure(tc.status, tc.body, "grok-4.5")
			if tc.class == ClassNone {
				if d.ShouldCooldown {
					t.Fatalf("must not cool: %+v", d)
				}
				return
			}
			if !d.ShouldCooldown {
				t.Fatalf("expected cooldown for %q: %+v", tc.body, d)
			}
			if d.Class != tc.class {
				t.Fatalf("class=%q want=%q body=%q", d.Class, tc.class, tc.body)
			}
			if tc.class == ClassFreeUsage {
				if d.BlockModel {
					t.Fatalf("free-usage must not block model: %+v", d)
				}
				if d.Model == "" {
					t.Fatalf("expected model fallback: %+v", d)
				}
			}
		})
	}
}

func TestShouldEnterCooldownPoolTextFirst(t *testing.T) {
	if !ShouldEnterCooldownPool(0, "额度用完") {
		t.Fatal("额度用完 must enter cooldown pool")
	}
	if !ShouldEnterCooldownPool(200, `{"code":"subscription:free-usage-exhausted"}`) {
		t.Fatal("code-only free-usage must enter cooldown pool even on status 200")
	}
	if ShouldEnterCooldownPool(400, "invalid_request_error: max_tokens required") {
		t.Fatal("validation must not enter cooldown pool")
	}
}

func TestFreeUsageGrok45BuildFreeCoolOnly(t *testing.T) {
	body := `{"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for model grok-4.5-build-free for now. Usage resets over a rolling 24-hour window — tokens (actual/limit): 1056720/1000000. Upgrade to a Grok subscription for higher limits: https://grok.com/supergrok"}`
	d := ClassifyUpstreamFailure(429, body, "grok-4.5")
	if !d.ShouldCooldown || d.Class != ClassFreeUsage {
		t.Fatalf("decision=%+v", d)
	}
	if d.BlockModel {
		t.Fatalf("free-usage must not soft-block model: %+v", d)
	}
	if d.Until == nil {
		t.Fatal("expected cooldown until")
	}
}
