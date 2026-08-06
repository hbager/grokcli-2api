package outboundproxy

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/hm2899/grokcli-2api/internal/config"
)

func TestSelectorParsesPoolAndSharedCredentials(t *testing.T) {
	current := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://proxy.example:8080",
		OutboundProxyUsername:   "user@name",
		OutboundProxyPassword:   "p:/?",
		OutboundProxyStrategy:   "sticky",
	}
	selector := New(func() config.Config { return current })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	proxyURL, err := selector.Proxy(request)
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL == nil || proxyURL.Scheme != "http" || proxyURL.Host != "proxy.example:8080" {
		t.Fatalf("proxy=%v", proxyURL)
	}
	password, _ := proxyURL.User.Password()
	if proxyURL.User.Username() != "user@name" || password != "p:/?" {
		t.Fatalf("userinfo=%v", proxyURL.User)
	}
}

func TestSelectorSupportsLegacyProxyFormats(t *testing.T) {
	current := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "proxy.example:8080:user:pass\nsocks5://proxy2.example:1080:user2:pass2",
		OutboundProxyStrategy:   "sticky",
	}
	selector := New(func() config.Config { return current })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	proxyURL, err := selector.Proxy(request)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := proxyURL.User.Password()
	if proxyURL.String() != "http://user:pass@proxy.example:8080" || password != "pass" {
		t.Fatalf("proxy=%v", proxyURL)
	}
}

func TestSelectorHonorsPinnedProxyIncludingDirect(t *testing.T) {
	current := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://proxy-a.example:8080\nhttp://proxy-b.example:8080",
		OutboundProxyStrategy:   "random",
	}
	selector := New(func() config.Config { return current })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	pinned, err := url.Parse("http://user:secret@pinned.example:8080")
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(WithPinnedProxy(request.Context(), pinned))
	for range 100 {
		selected, err := selector.Proxy(request)
		if err != nil || selected == nil || selected.String() != pinned.String() {
			t.Fatalf("pinned proxy=%v err=%v", selected, err)
		}
	}
	direct := request.WithContext(WithPinnedProxy(context.Background(), nil))
	selected, err := selector.Proxy(direct)
	if err != nil || selected != nil {
		t.Fatalf("direct binding proxy=%v err=%v", selected, err)
	}
}

func TestSelectorPinsRoundRobinByAccount(t *testing.T) {
	current := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    true,
		OutboundProxy:           "http://proxy-a.example:8080\nhttp://proxy-b.example:8080",
		OutboundProxyStrategy:   "round_robin",
	}
	selector := New(func() config.Config { return current })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	request = request.WithContext(WithAccountID(request.Context(), "account-1"))
	first, err := selector.Proxy(request)
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		next, err := selector.Proxy(request)
		if err != nil {
			t.Fatal(err)
		}
		if next.String() != first.String() {
			t.Fatalf("account proxy changed from %s to %s", first, next)
		}
	}
}

func TestSelectorHonorsExplicitDisableAndDynamicUpdates(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://env-proxy.example:8080")
	t.Setenv("HTTPS_PROXY", "http://env-proxy.example:8080")
	t.Setenv("NO_PROXY", "")
	current := config.Config{
		OutboundProxyConfigured: true,
		OutboundProxyEnabled:    false,
		OutboundProxy:           "http://configured.example:8080",
	}
	selector := New(func() config.Config { return current })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	proxyURL, err := selector.Proxy(request)
	if err != nil || proxyURL != nil {
		t.Fatalf("disabled proxy=%v err=%v", proxyURL, err)
	}

	current.OutboundProxyEnabled = true
	proxyURL, err = selector.Proxy(request)
	if err != nil || proxyURL == nil || proxyURL.Host != "configured.example:8080" {
		t.Fatalf("enabled proxy=%v err=%v", proxyURL, err)
	}
}

func TestSelectorUsesEnvironmentOnlyWhenUnconfigured(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://env-proxy.example:8080")
	t.Setenv("HTTPS_PROXY", "http://env-proxy.example:8080")
	t.Setenv("NO_PROXY", "")
	selector := New(func() config.Config { return config.Config{} })
	request := httptest.NewRequest("GET", "https://upstream.example/v1", nil)
	proxyURL, err := selector.Proxy(request)
	if err != nil || proxyURL == nil || proxyURL.Host != "env-proxy.example:8080" {
		t.Fatalf("environment proxy=%v err=%v", proxyURL, err)
	}
}
