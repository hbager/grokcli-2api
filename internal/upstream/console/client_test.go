package console

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

func TestNormalizeConsoleBodyStripsCacheFields(t *testing.T) {
	body := map[string]any{
		"messages":         []any{map[string]any{"role": "user", "content": "hi"}},
		"prompt_cache_key": "x",
		"tools":            []any{map[string]any{"type": "x_search"}},
	}
	out := normalizeConsoleBody(body, "grok-4.3")
	if out["model"] != "grok-4.3" {
		t.Fatalf("model=%v", out["model"])
	}
	if _, ok := out["prompt_cache_key"]; ok {
		t.Fatal("prompt_cache_key should be stripped")
	}
	if tools, ok := out["tools"].([]any); ok {
		for _, raw := range tools {
			m, _ := raw.(map[string]any)
			if m["type"] == "x_search" {
				t.Fatal("x_search should be removed")
			}
		}
	}
	if out["store"] != false {
		t.Fatalf("store=%v", out["store"])
	}
}

func TestConsoleOpenUsesSSOCookie(t *testing.T) {
	var gotCookie string
	var gotAuth string
	var gotProof string
	var gotCluster string
	var gotModel string
	payload := "event: response.completed\ndata: " + `{"type":"response.completed","response":{"id":"r1","model":"grok-4.3","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		if r.URL.Path == "/v1/dpop/token" {
			serveTestDPoPToken(t, w, r, 1)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		gotProof = r.Header.Get("DPoP")
		gotCluster = r.Header.Get("x-cluster")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotModel, _ = body["model"].(string)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, payload)
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "a", SSO: "eyJtest"}, "grok-4.3", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.Contains(gotCookie, "sso=eyJtest") {
		t.Fatalf("cookie=%q", gotCookie)
	}
	if !strings.HasPrefix(gotAuth, "DPoP ") || strings.TrimSpace(strings.TrimPrefix(gotAuth, "DPoP ")) == "" {
		t.Fatalf("auth=%q", gotAuth)
	}
	if gotProof == "" {
		t.Fatal("missing DPoP proof")
	}
	if gotCluster != "https://us-east-1.api.x.ai" {
		t.Fatalf("responses x-cluster=%q", gotCluster)
	}
	if gotModel != "grok-4.3" {
		t.Fatalf("model=%q", gotModel)
	}
}

func TestNormalizeConsoleBodyMultiAgentXHigh(t *testing.T) {
	body := map[string]any{
		"messages":         []any{map[string]any{"role": "user", "content": "hi"}},
		"max_tokens":       99,
		"reasoning_effort": "low",
	}
	out := normalizeConsoleBody(body, "grok-4.20-multi-agent", "xhigh")
	if out["model"] != "grok-4.20-multi-agent" {
		t.Fatalf("model=%v", out["model"])
	}
	if _, ok := out["max_output_tokens"]; ok {
		t.Fatal("max_output_tokens should be stripped for multi-agent")
	}
	r, _ := out["reasoning"].(map[string]any)
	if r == nil || r["effort"] != "xhigh" {
		t.Fatalf("reasoning=%#v want effort=xhigh", out["reasoning"])
	}
}

func TestConsoleOpenMergesCloudflareCookies(t *testing.T) {
	var gotCookie string
	payload := "event: response.completed\ndata: " + `{"type":"response.completed","response":{"id":"r1","model":"grok-4.3","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		if r.URL.Path == "/v1/dpop/token" {
			serveTestDPoPToken(t, w, r, 1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, payload)
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{
		ID:  "a",
		SSO: "eyJtest",
		Cookies: map[string]string{
			"cf_clearance": "clear-1",
			"__cf_bm":      "bm-1",
		},
	}, "grok-4.3", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.Contains(gotCookie, "sso=eyJtest") {
		t.Fatalf("cookie missing sso: %q", gotCookie)
	}
	if !strings.Contains(gotCookie, "cf_clearance=clear-1") {
		t.Fatalf("cookie missing cf_clearance: %q", gotCookie)
	}
	if !strings.Contains(gotCookie, "__cf_bm=bm-1") {
		t.Fatalf("cookie missing __cf_bm: %q", gotCookie)
	}
}

func serveTestDPoPToken(t *testing.T, w http.ResponseWriter, r *http.Request, sequence int) string {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Fatalf("mint method=%s", r.Method)
	}
	if r.Header.Get("Authorization") != "" {
		t.Fatalf("mint must use SSO cookie, got auth=%q", r.Header.Get("Authorization"))
	}
	if r.Header.Get("x-cluster") != "" {
		t.Fatalf("mint must not send x-cluster, got %q", r.Header.Get("x-cluster"))
	}
	if !strings.Contains(r.Header.Get("Cookie"), "sso=") {
		t.Fatalf("mint cookie=%q", r.Header.Get("Cookie"))
	}
	var request struct {
		JWK dpopJWK `json:"jwk"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Fatalf("decode mint: %v", err)
	}
	jkt, err := dpopJWKThumbprint(request.JWK)
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := json.Marshal(map[string]any{
		"exp": time.Now().Add(5 * time.Minute).Unix(),
		"cnf": map[string]any{"jkt": jkt},
		"seq": sequence,
	})
	access := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." +
		base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": access,
		"token_type":   "DPoP",
		"expires_in":   300,
	})
	return access
}

func decodeAndVerifyProof(t *testing.T, proof, access, method, target string) (map[string]any, map[string]any) {
	t.Helper()
	parts := strings.Split(proof, ".")
	if len(parts) != 3 {
		t.Fatalf("proof has %d parts", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != 64 {
		t.Fatalf("signature length=%d err=%v", len(signature), err)
	}
	var header map[string]any
	var claims map[string]any
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		t.Fatal(err)
	}
	jwkMap, ok := header["jwk"].(map[string]any)
	if !ok || header["alg"] != "ES256" || header["typ"] != "dpop+jwt" {
		t.Fatalf("proof header=%#v", header)
	}
	proofJWK := dpopJWK{Kty: fmt.Sprint(jwkMap["kty"]), Crv: fmt.Sprint(jwkMap["crv"]), X: fmt.Sprint(jwkMap["x"]), Y: fmt.Sprint(jwkMap["y"])}
	if proofJWK.Kty != "EC" || proofJWK.Crv != "P-256" {
		t.Fatalf("proof JWK=%#v", proofJWK)
	}
	proofJKT, err := dpopJWKThumbprint(proofJWK)
	if err != nil {
		t.Fatal(err)
	}
	accessParts := strings.Split(access, ".")
	if len(accessParts) != 3 {
		t.Fatalf("access token has %d parts", len(accessParts))
	}
	accessClaimsJSON, err := base64.RawURLEncoding.DecodeString(accessParts[1])
	if err != nil {
		t.Fatal(err)
	}
	var accessClaims struct {
		CNF struct {
			JKT string `json:"jkt"`
		} `json:"cnf"`
	}
	if err := json.Unmarshal(accessClaimsJSON, &accessClaims); err != nil || accessClaims.CNF.JKT != proofJKT {
		t.Fatalf("access token is not bound to proof JWK: err=%v cnf=%q jkt=%q", err, accessClaims.CNF.JKT, proofJKT)
	}
	xBytes, _ := base64.RawURLEncoding.DecodeString(proofJWK.X)
	yBytes, _ := base64.RawURLEncoding.DecodeString(proofJWK.Y)
	publicKey := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(xBytes), Y: new(big.Int).SetBytes(yBytes)}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(publicKey, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		t.Fatal("invalid proof signature")
	}
	accessDigest := sha256.Sum256([]byte(access))
	if claims["ath"] != base64.RawURLEncoding.EncodeToString(accessDigest[:]) || claims["htm"] != method || claims["htu"] != target {
		t.Fatalf("proof claims=%#v", claims)
	}
	if strings.TrimSpace(fmt.Sprint(claims["jti"])) == "" {
		t.Fatal("empty jti")
	}
	return header, claims
}

func TestConsoleDPoPProofAndCache(t *testing.T) {
	var mintCount atomic.Int32
	var responseCount atomic.Int32
	var mu sync.Mutex
	proofs := make([]string, 0, 2)
	accesses := make([]string, 0, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/dpop/token":
			serveTestDPoPToken(t, w, r, int(mintCount.Add(1)))
		case "/v1/responses":
			responseCount.Add(1)
			access := strings.TrimPrefix(r.Header.Get("Authorization"), "DPoP ")
			mu.Lock()
			proofs = append(proofs, r.Header.Get("DPoP"))
			accesses = append(accesses, access)
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	account := pool.ConsoleAccount{ID: "cache-account", SSO: "cache-sso"}
	for i := 0; i < 2; i++ {
		resp, err := client.Open(t.Context(), account, "grok-4.3", map[string]any{"messages": []any{}})
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	if mintCount.Load() != 1 || responseCount.Load() != 2 {
		t.Fatalf("mints=%d responses=%d", mintCount.Load(), responseCount.Load())
	}
	if accesses[0] != accesses[1] || proofs[0] == proofs[1] {
		t.Fatalf("access reuse/proof uniqueness failed")
	}
	decodeAndVerifyProof(t, proofs[0], accesses[0], "POST", srv.URL+"/v1/responses")
	decodeAndVerifyProof(t, proofs[1], accesses[1], "POST", srv.URL+"/v1/responses")
}

func TestConsoleDPoPConcurrentMintCoalesced(t *testing.T) {
	var mintCount atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dpop/token" {
			if mintCount.Add(1) == 1 {
				close(started)
			}
			<-release
			serveTestDPoPToken(t, w, r, 1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	account := pool.ConsoleAccount{ID: "same", SSO: "same-sso"}
	errs := make(chan error, 8)
	for i := 0; i < cap(errs); i++ {
		go func() {
			resp, err := client.Open(t.Context(), account, "grok-4.3", map[string]any{})
			if resp != nil {
				_ = resp.Body.Close()
			}
			errs <- err
		}()
	}
	<-started
	close(release)
	for i := 0; i < cap(errs); i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if mintCount.Load() != 1 {
		t.Fatalf("mint count=%d", mintCount.Load())
	}
}

func TestConsoleDPoP401RemintsOnce(t *testing.T) {
	var mintCount atomic.Int32
	var requestCount atomic.Int32
	var accesses []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dpop/token" {
			serveTestDPoPToken(t, w, r, int(mintCount.Add(1)))
			return
		}
		mu.Lock()
		accesses = append(accesses, r.Header.Get("Authorization"))
		mu.Unlock()
		if requestCount.Add(1) == 1 {
			http.Error(w, "stale", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "retry", SSO: "retry-sso"}, "grok-4.3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if mintCount.Load() != 2 || requestCount.Load() != 2 || accesses[0] == accesses[1] {
		t.Fatalf("mints=%d requests=%d accesses=%v", mintCount.Load(), requestCount.Load(), accesses)
	}
}

func TestConsoleDPoPConcurrent401CoalescesRemintAndIgnoresLateStale401(t *testing.T) {
	var mintCount atomic.Int32
	var oldRequests atomic.Int32
	var newRequests atomic.Int32
	oldArrived := make(chan struct{}, 2)
	releaseFirst401 := make(chan struct{})
	releaseLate401 := make(chan struct{})
	replacementUsed := make(chan struct{})
	remintStarted := make(chan struct{})
	releaseRemint := make(chan struct{})
	var replacementOnce sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dpop/token" {
			sequence := int(mintCount.Add(1))
			if sequence == 2 {
				close(remintStarted)
				<-releaseRemint
			}
			serveTestDPoPToken(t, w, r, sequence)
			return
		}
		access := strings.TrimPrefix(r.Header.Get("Authorization"), "DPoP ")
		parts := strings.Split(access, ".")
		claimsJSON, _ := base64.RawURLEncoding.DecodeString(parts[1])
		var claims map[string]any
		_ = json.Unmarshal(claimsJSON, &claims)
		if int(claims["seq"].(float64)) == 1 {
			requestNumber := oldRequests.Add(1)
			oldArrived <- struct{}{}
			if requestNumber == 1 {
				<-releaseFirst401
			} else {
				<-releaseLate401
			}
			http.Error(w, "stale", http.StatusUnauthorized)
			return
		}
		newRequests.Add(1)
		replacementOnce.Do(func() { close(replacementUsed) })
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	account := pool.ConsoleAccount{ID: "concurrent-401", SSO: "same-sso"}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			resp, err := client.Open(t.Context(), account, "grok-4.3", map[string]any{})
			if resp != nil {
				_ = resp.Body.Close()
			}
			errs <- err
		}()
	}
	<-oldArrived
	<-oldArrived
	close(releaseFirst401)
	select {
	case <-remintStarted:
	case <-time.After(time.Second):
		t.Fatal("remint did not start")
	}
	close(releaseLate401)
	close(releaseRemint)
	select {
	case <-replacementUsed:
	case <-time.After(time.Second):
		t.Fatal("replacement token was not used")
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if mintCount.Load() != 2 {
		t.Fatalf("mints=%d, want initial plus one coalesced remint", mintCount.Load())
	}
	if oldRequests.Load() != 2 || newRequests.Load() != 2 {
		t.Fatalf("old requests=%d replacement requests=%d", oldRequests.Load(), newRequests.Load())
	}
	resp, err := client.Open(t.Context(), account, "grok-4.3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if mintCount.Load() != 2 {
		t.Fatalf("late old-token 401 evicted replacement: mints=%d", mintCount.Load())
	}
}

func TestDPoPClockSkewAndCanonicalHTU(t *testing.T) {
	local := time.Date(2026, 8, 5, 17, 0, 0, 0, time.UTC)
	if got := dpopClockSkewFromDateHeader("", local, local); got != 0 {
		t.Fatalf("empty Date skew=%v", got)
	}
	if got := dpopClockSkewFromDateHeader("not-a-date", local, local); got != 0 {
		t.Fatalf("invalid Date skew=%v", got)
	}
	if got := dpopClockSkewFromDateHeader(local.Add(-80*time.Second).Format(http.TimeFormat), local, local); got != -80*time.Second {
		t.Fatalf("negative skew=%v", got)
	}
	if got := dpopClockSkewFromDateHeader(local.Add(90*time.Second).Format(http.TimeFormat), local, local); got != 90*time.Second {
		t.Fatalf("positive skew=%v", got)
	}
	if got := dpopProofIAT(dpopSession{clockSkew: -80 * time.Second}, local); got != local.Add(-80*time.Second).Unix() {
		t.Fatalf("skewed iat=%d", got)
	}
	if got := dpopProofIAT(dpopSession{}, local); got != local.Unix() {
		t.Fatalf("zero-skew iat=%d", got)
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicJWK := publicDPoPJWK(&privateKey.PublicKey)
	jkt, err := dpopJWKThumbprint(publicJWK)
	if err != nil {
		t.Fatal(err)
	}
	accessClaims, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Minute).Unix(), "cnf": map[string]any{"jkt": jkt}})
	access := "header." + base64.RawURLEncoding.EncodeToString(accessClaims) + ".signature"
	session := dpopSession{accessToken: access, privateKey: privateKey, publicJWK: publicJWK, clockSkew: -80 * time.Second}
	req := httptest.NewRequest(http.MethodPost, "https://console.x.ai/v1/responses?ignored=yes", nil)
	if err := applyDPoPAuthorization(req, session); err != nil {
		t.Fatal(err)
	}
	_, claims := decodeAndVerifyProof(t, req.Header.Get("DPoP"), access, "POST", "https://console.x.ai/v1/responses")
	iat := int64(claims["iat"].(float64))
	want := time.Now().Add(-80 * time.Second).Unix()
	if iat < want-2 || iat > want+2 {
		t.Fatalf("iat=%d want about %d", iat, want)
	}
}

func TestDPoPClockSkewMidpointAndHalfSecondRounding(t *testing.T) {
	server := time.Date(2026, 8, 5, 17, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		before time.Time
		after  time.Time
		want   time.Duration
	}{
		{name: "midpoint", before: server.Add(-3 * time.Second), after: server.Add(-time.Second), want: 2 * time.Second},
		{name: "positive below half", before: server.Add(-499 * time.Millisecond), after: server.Add(-499 * time.Millisecond), want: 0},
		{name: "positive half", before: server.Add(-500 * time.Millisecond), after: server.Add(-500 * time.Millisecond), want: time.Second},
		{name: "positive above half", before: server.Add(-501 * time.Millisecond), after: server.Add(-501 * time.Millisecond), want: time.Second},
		{name: "negative below half", before: server.Add(499 * time.Millisecond), after: server.Add(499 * time.Millisecond), want: 0},
		{name: "negative half", before: server.Add(500 * time.Millisecond), after: server.Add(500 * time.Millisecond), want: -time.Second},
		{name: "negative above half", before: server.Add(501 * time.Millisecond), after: server.Add(501 * time.Millisecond), want: -time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := dpopClockSkewFromDateHeader(server.Format(http.TimeFormat), test.before, test.after)
			if got != test.want {
				t.Fatalf("skew=%v, want %v", got, test.want)
			}
		})
	}
}

func (m *dpopSessionManager) waitersForTest(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if load := m.loads[key]; load != nil {
		return load.waiters
	}
	return 0
}

func TestDPoPManagerLiveWaiterRetriesCanceledLeader(t *testing.T) {
	var manager dpopSessionManager
	var fetches atomic.Int32
	leaderStarted := make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderResult := make(chan error, 1)
	go func() {
		_, err := manager.get(leaderCtx, "key", func(ctx context.Context) (dpopSession, error) {
			if fetches.Add(1) != 1 {
				t.Error("leader was not first fetch")
			}
			close(leaderStarted)
			<-ctx.Done()
			return dpopSession{}, ctx.Err()
		})
		leaderResult <- err
	}()
	<-leaderStarted

	waiterFetch := make(chan struct{})
	waiterResult := make(chan error, 1)
	go func() {
		_, err := manager.get(t.Context(), "key", func(context.Context) (dpopSession, error) {
			fetches.Add(1)
			close(waiterFetch)
			return dpopSession{accessToken: "replacement", expiresAt: time.Now().Add(time.Minute)}, nil
		})
		waiterResult <- err
	}()
	deadline := time.After(time.Second)
	for manager.waitersForTest("key") == 0 {
		select {
		case <-deadline:
			t.Fatal("waiter did not join leader")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancelLeader()
	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error=%v", err)
	}
	select {
	case <-waiterFetch:
	case <-time.After(time.Second):
		t.Fatal("live waiter did not retry mint")
	}
	if err := <-waiterResult; err != nil {
		t.Fatalf("waiter error=%v", err)
	}
	if fetches.Load() != 2 {
		t.Fatalf("fetches=%d, want 2", fetches.Load())
	}
}

func TestDPoPCacheKeyIsolation(t *testing.T) {
	client := &Client{BaseURL: "https://console.example"}
	first := client.dpopCacheKey(pool.ConsoleAccount{ID: "a", SSO: "sso-1"})
	if first == client.dpopCacheKey(pool.ConsoleAccount{ID: "b", SSO: "sso-1"}) {
		t.Fatal("account ID must isolate DPoP sessions")
	}
	if first == client.dpopCacheKey(pool.ConsoleAccount{ID: "a", SSO: "sso-2"}) {
		t.Fatal("SSO token must isolate DPoP sessions")
	}
	if first == (&Client{BaseURL: "https://other.example"}).dpopCacheKey(pool.ConsoleAccount{ID: "a", SSO: "sso-1"}) {
		t.Fatal("base URL must isolate DPoP sessions")
	}
	if first == client.dpopCacheKey(pool.ConsoleAccount{ID: "a", SSO: "sso-1"}, "http://user:secret@proxy-a.example:8080") {
		t.Fatal("egress must isolate DPoP sessions")
	}
	credentialed := client.dpopCacheKey(pool.ConsoleAccount{ID: "a", SSO: "sso-1"}, "http://user:secret@proxy-a.example:8080")
	if strings.Contains(credentialed, "user") || strings.Contains(credentialed, "secret") || strings.Contains(credentialed, "proxy-a") {
		t.Fatal("cache key must hash the canonical egress identity")
	}
	if strings.Contains(first, "sso-1") {
		t.Fatal("cache key must not contain the SSO token")
	}
}

func TestConsoleDPoPUsesAccountContextForEveryProxyLookup(t *testing.T) {
	cfg := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://proxy-a.example:80\nhttp://proxy-b.example:80",
		OutboundProxyStrategy:   "round_robin",
	}
	accountID := ""
	expected := ""
	for i := 0; expected == "" || expected == "http://proxy-a.example:80"; i++ {
		accountID = fmt.Sprintf("proxy-account-%d", i)
		probe := httptest.NewRequest(http.MethodGet, "https://example.test", nil)
		probe = probe.WithContext(outboundproxy.WithAccountID(probe.Context(), accountID))
		selected, _ := outboundproxy.New(func() config.Config { return cfg }).Proxy(probe)
		expected = selected.String()
	}

	selector := outboundproxy.New(func() config.Config { return cfg })
	var mu sync.Mutex
	var seen []string
	proxy := func(r *http.Request) (*url.URL, error) {
		selected, err := selector.Proxy(r)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		seen = append(seen, selected.String())
		mu.Unlock()
		return nil, nil
	}
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.Proxy = proxy
	httpClient := &http.Client{Transport: baseTransport}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dpop/token" {
			serveTestDPoPToken(t, w, r, 1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: httpClient, DisableBrowserTLS: true}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: accountID, SSO: "proxy-sso"}, "grok-4.3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != expected {
		t.Fatalf("proxy selections=%v, want one resolved %q selection", seen, expected)
	}
}

func TestConsoleDPoPRandomProxyPinnedAndCacheIsolatedByEgress(t *testing.T) {
	cfg := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://user:secret@proxy-a.example:80\nhttp://user:secret@proxy-b.example:80",
		OutboundProxyStrategy:   "random",
	}
	selector := outboundproxy.New(func() config.Config { return cfg })
	var resolveCount atomic.Int32
	var mu sync.Mutex
	seen := make(map[string][]string)
	resolver := func(r *http.Request) (*url.URL, error) {
		resolveCount.Add(1)
		return selector.Proxy(r)
	}
	doer := roundTripDoer(func(req *http.Request) (*http.Response, error) {
		proxyURL, err := selector.Proxy(req)
		if err != nil {
			return nil, err
		}
		identity := "direct"
		if proxyURL != nil {
			identity = proxyURL.String()
		}
		mu.Lock()
		seen[req.Header.Get("X-Flow")] = append(seen[req.Header.Get("X-Flow")], identity)
		mu.Unlock()
		body := io.NopCloser(strings.NewReader("event: response.completed\ndata: {}\n\n"))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body, Request: req}, nil
	})
	client := &Client{BaseURL: "https://console.example", Proxy: resolver, DisableBrowserTLS: true}
	account := pool.ConsoleAccount{ID: "random-account", SSO: "random-sso"}
	ctx := outboundproxy.WithAccountID(t.Context(), account.ID)
	keys := make(map[string]bool)
	for flow := range 40 {
		pinnedCtx, identity, err := client.pinEgress(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys[client.dpopCacheKey(account, identity)] = true
		for _, path := range []string{"/v1/dpop/token", "/v1/responses"} {
			req, err := http.NewRequestWithContext(pinnedCtx, http.MethodPost, client.base()+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Flow", fmt.Sprint(flow))
			resp, err := doer.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
		}
	}
	if resolveCount.Load() != 40 {
		t.Fatalf("resolves=%d, want one per flow", resolveCount.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for flow, selections := range seen {
		if len(selections) != 2 || selections[0] != selections[1] {
			t.Fatalf("flow %s selections=%v", flow, selections)
		}
	}
	if len(keys) != 2 {
		t.Fatalf("cache keys=%d, want one per random egress", len(keys))
	}
	for key := range keys {
		if strings.Contains(key, "user") || strings.Contains(key, "secret") || strings.Contains(key, "proxy-") {
			t.Fatalf("cache key leaks proxy credentials or address: %q", key)
		}
	}
}

type roundTripDoer func(*http.Request) (*http.Response, error)

func (f roundTripDoer) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestConsoleDPoPMintDateCorrectsProofIAT(t *testing.T) {
	var proofIAT int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dpop/token" {
			w.Header().Set("Date", time.Now().UTC().Add(-30*time.Second).Format(http.TimeFormat))
			serveTestDPoPToken(t, w, r, 1)
			return
		}
		parts := strings.Split(r.Header.Get("DPoP"), ".")
		claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims map[string]any
		if err := json.Unmarshal(claimsJSON, &claims); err != nil {
			t.Fatal(err)
		}
		proofIAT = int64(claims["iat"].(float64))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer srv.Close()
	client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
	resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "skew", SSO: "skew-sso"}, "grok-4.3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if delta := time.Now().Unix() - proofIAT; delta < 20 || delta > 40 {
		t.Fatalf("proof iat delta=%ds, want about 30s", delta)
	}
}

func TestConsoleDPoPRejectsInvalidMintResponse(t *testing.T) {
	tests := []struct {
		name       string
		tokenType  string
		expiresIn  int
		thumbprint string
	}{
		{name: "token type", tokenType: "Bearer", expiresIn: 300},
		{name: "lifetime", tokenType: "DPoP", expiresIn: 3601},
		{name: "key binding", tokenType: "DPoP", expiresIn: 300, thumbprint: "wrong-key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					JWK dpopJWK `json:"jwk"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				jkt := test.thumbprint
				if jkt == "" {
					jkt, _ = dpopJWKThumbprint(request.JWK)
				}
				claims, _ := json.Marshal(map[string]any{"exp": time.Now().Add(2 * time.Hour).Unix(), "cnf": map[string]any{"jkt": jkt}})
				access := "header." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
				_ = json.NewEncoder(w).Encode(map[string]any{"access_token": access, "token_type": test.tokenType, "expires_in": test.expiresIn})
			}))
			defer srv.Close()
			client := &Client{BaseURL: srv.URL, HTTP: srv.Client(), DisableBrowserTLS: true}
			resp, err := client.Open(t.Context(), pool.ConsoleAccount{ID: "invalid", SSO: "invalid-sso"}, "grok-4.3", map[string]any{})
			if resp != nil {
				_ = resp.Body.Close()
			}
			if err == nil {
				t.Fatal("expected invalid mint response error")
			}
		})
	}
}
