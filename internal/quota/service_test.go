package quota

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestSetProxyConfiguresQuotaTransportWithoutEnvOverride(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://env-proxy.example:8080")
	t.Setenv("HTTPS_PROXY", "http://env-proxy.example:8080")
	t.Setenv("NO_PROXY", "")
	service := New(nil, "http://example.invalid")
	var calls atomic.Int64
	service.SetProxy(func(*http.Request) (*url.URL, error) {
		calls.Add(1)
		return nil, nil
	})
	transport, ok := service.client().Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatalf("transport=%T", service.client().Transport)
	}
	if _, err := transport.Proxy(httptest.NewRequest("GET", "https://upstream.example", nil)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("proxy calls=%d", calls.Load())
	}
}
