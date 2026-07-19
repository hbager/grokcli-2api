package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/store/postgres"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

type Service struct {
	Store      *postgres.Connector
	Upstream   string
	Workers    int
	httpClient *http.Client
}

func New(store *postgres.Connector, upstream string) *Service {
	return &Service{
		Store:      store,
		Upstream:   strings.TrimRight(upstream, "/"),
		Workers:    envInt("GROK2API_QUOTA_WORKERS", 8, 1, 32),
		httpClient: newQuotaHTTPClient(http.ProxyFromEnvironment),
	}
}

func newQuotaHTTPClient(proxy func(*http.Request) (*url.URL, error)) *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			Proxy:                 proxy,
			MaxIdleConns:          128,
			MaxIdleConnsPerHost:   64,
			MaxConnsPerHost:       96,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   8 * time.Second,
			ResponseHeaderTimeout: 12 * time.Second,
			ForceAttemptHTTP2:     true,
			DialContext: (&net.Dialer{
				Timeout:   6 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}

func (s *Service) client() *http.Client {
	if s != nil && s.httpClient != nil {
		return s.httpClient
	}
	return newQuotaHTTPClient(http.ProxyFromEnvironment)
}

func (s *Service) SetProxy(proxy func(*http.Request) (*url.URL, error)) {
	if s == nil {
		return
	}
	previous := s.httpClient
	s.httpClient = newQuotaHTTPClient(proxy)
	if previous != nil {
		previous.CloseIdleConnections()
	}
}

func (s *Service) FetchCached(ctx context.Context) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	return s.Store.ListCachedQuotas(ctx)
}

func (s *Service) FetchOne(ctx context.Context, accountID string) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	auth, err := s.Store.GetAccountAuth(ctx, accountID)
	if err != nil || auth == nil {
		return map[string]any{"ok": false, "account_id": accountID, "error": "account not found or has no token"}, nil
	}
	item := s.fetchOne(ctx, *auth)
	// SaveQuotaSnapshot writes last_quota AND syncs pool status (quota_disabled / re-enable).
	_ = s.Store.SaveQuotaSnapshot(ctx, auth.ID, item)
	if item["exhausted"] == true {
		item["auto_disabled"] = true
		item["pool_disabled"] = true
	}
	return item, nil
}

func (s *Service) FetchAll(ctx context.Context) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	// Include disabled accounts so recovery can re-enable them after billing heals.
	auths, err := s.Store.ListAccountAuths(ctx, 2000, false)
	if err != nil {
		return nil, err
	}
	workers := s.Workers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(auths) && len(auths) > 0 {
		workers = len(auths)
	}
	type result struct{ item map[string]any }
	ch := make(chan result, len(auths))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, auth := range auths {
		wg.Add(1)
		go func(a postgres.AccountAuth) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			item := s.fetchOne(ctx, a)
			// Real-time DB sync: last_quota + disabled_for_quota / pool_status / re-enable.
			_ = s.Store.SaveQuotaSnapshot(ctx, a.ID, item)
			if item["exhausted"] == true {
				item["auto_disabled"] = true
				item["pool_disabled"] = true
			}
			ch <- result{item: item}
		}(auth)
	}
	wg.Wait()
	close(ch)
	results := make([]map[string]any, 0, len(auths))
	for r := range ch {
		results = append(results, r.item)
	}
	okCount, exhausted, autoDisabled, poolDisabled := 0, 0, 0, 0
	var totalUsed, totalLimit, totalRemaining float64
	activeOK := 0
	for _, r := range results {
		if r["ok"] == true {
			okCount++
		}
		if r["exhausted"] == true {
			exhausted++
		}
		if r["auto_disabled"] == true {
			autoDisabled++
		}
		if r["pool_disabled"] == true {
			poolDisabled++
		}
		if r["ok"] == true && r["pool_disabled"] != true && r["exhausted"] != true {
			activeOK++
			totalUsed += floatOf(r["used"])
			totalLimit += floatOf(r["monthly_limit"])
			totalRemaining += floatOf(r["remaining"])
		}
	}
	return map[string]any{
		"ok":                  true,
		"fetched_at":          time.Now().Unix(),
		"count":               len(results),
		"ok_count":            okCount,
		"exhausted_count":     exhausted,
		"auto_disabled_count": autoDisabled,
		"pool_disabled_count": poolDisabled,
		"active_ok_count":     activeOK,
		"total_used":          totalUsed,
		"total_monthly_limit": totalLimit,
		"total_remaining":     totalRemaining,
		"workers":             workers,
		"accounts":            results,
	}, nil
}

func (s *Service) fetchOne(ctx context.Context, auth postgres.AccountAuth) map[string]any {
	ctx = outboundproxy.WithAccountID(ctx, auth.ID)
	out := map[string]any{
		"ok":         false,
		"account_id": auth.ID,
		"email":      auth.Email,
		"fetched_at": time.Now().Unix(),
		"source":     "billing",
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Upstream+"/billing", nil)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	// Reuse grok headers style.
	gc := &grok.Client{BaseURL: s.Upstream}
	for k, v := range gc.Headers(auth.Token, "grok-4.5") {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client().Do(req)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		out["error"] = fmt.Sprintf("billing HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
		out["status_code"] = resp.StatusCode
		return out
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		out["error"] = "parse billing: " + err.Error()
		return out
	}
	norm := normalizeBilling(raw)
	for k, v := range norm {
		out[k] = v
	}
	out["ok"] = norm["ok"] != false && norm["error"] == nil
	return out
}

func envInt(name string, fallback, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func normalizeBilling(raw map[string]any) map[string]any {
	if raw == nil {
		return map[string]any{"ok": false, "error": "empty billing response"}
	}
	cfg := raw
	if nested, ok := raw["config"].(map[string]any); ok {
		cfg = nested
	}
	monthly := money(cfg["monthlyLimit"])
	if monthly == nil {
		monthly = money(cfg["monthly_limit"])
	}
	used := money(cfg["used"])
	var remaining *float64
	if monthly != nil && used != nil {
		v := *monthly - *used
		if v < 0 {
			v = 0
		}
		remaining = &v
	}
	exhausted := false
	if monthly != nil && used != nil && *monthly > 0 && *used >= *monthly {
		exhausted = true
	}
	out := map[string]any{
		"ok":            true,
		"monthly_limit": monthly,
		"used":          used,
		"remaining":     remaining,
		"exhausted":     exhausted,
		"raw":           raw,
	}
	if monthly != nil && used != nil {
		out["display"] = map[string]any{
			"summary": fmt.Sprintf("%s / %s", fmtUSD(used), fmtUSD(monthly)),
		}
	}
	return out
}

func money(v any) *float64 {
	switch t := v.(type) {
	case float64:
		return &t
	case int:
		f := float64(t)
		return &f
	case int64:
		f := float64(t)
		return &f
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return &f
		}
	case map[string]any:
		if val, ok := t["val"]; ok {
			return money(val)
		}
	}
	return nil
}

func fmtUSD(v *float64) string {
	if v == nil {
		return "$0.00"
	}
	return fmt.Sprintf("$%.2f", *v)
}

func floatOf(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case *float64:
		if t == nil {
			return 0
		}
		return *t
	default:
		return 0
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func quotaTransport() http.RoundTripper {
	// Prefer explicit env proxy; DefaultTransport already honors HTTP(S)_PROXY.
	proxyURL := firstNonEmpty(
		os.Getenv("GROK2API_XAI_PROXY"),
		os.Getenv("GROK2API_PROXY"),
		os.Getenv("HTTPS_PROXY"),
		os.Getenv("HTTP_PROXY"),
		os.Getenv("https_proxy"),
		os.Getenv("http_proxy"),
	)
	base, _ := http.DefaultTransport.(*http.Transport)
	if base == nil {
		return http.DefaultTransport
	}
	tr := base.Clone()
	if proxyURL == "" {
		return tr
	}
	u, err := url.Parse(proxyURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return tr
	}
	tr.Proxy = http.ProxyURL(u)
	return tr
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
