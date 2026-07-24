// Package browserhttp provides a Cloudflare-tolerant HTTP client using
// Chrome TLS/HTTP2 fingerprint impersonation (bogdanfinn/tls-client).
package browserhttp

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

// Client is a drop-in Do()-style client with Chrome TLS fingerprint.
// ProxyURL is applied per-request via SetProxy (thread-safe).
type Client struct {
	// Profile selects a browser TLS profile; empty → Chrome_131.
	Profile string
	// Timeout bounds the whole request (tls-client requires a timeout).
	// 0 → 120s (console streams may run long; raise if needed).
	Timeout time.Duration
	// ProxyURL optional static proxy (http://user:pass@host:port).
	ProxyURL string
	// Proxy optional dynamic resolver (std net/http signature).
	Proxy func(*http.Request) (*url.URL, error)

	mu     sync.Mutex
	inner  tls_client.HttpClient
	curPx  string
	initOnce sync.Once
	initErr  error
}

func (c *Client) ensure() error {
	c.initOnce.Do(func() {
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = 120 * time.Second
		}
		opts := []tls_client.HttpClientOption{
			tls_client.WithTimeoutMilliseconds(int(timeout / time.Millisecond)),
			tls_client.WithClientProfile(resolveProfile(c.Profile)),
			tls_client.WithRandomTLSExtensionOrder(),
			tls_client.WithNotFollowRedirects(),
		}
		if px := strings.TrimSpace(c.ProxyURL); px != "" {
			opts = append(opts, tls_client.WithProxyUrl(px))
			c.curPx = px
		}
		inner, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
		if err != nil {
			c.initErr = err
			return
		}
		c.inner = inner
	})
	return c.initErr
}

func resolveProfile(name string) profiles.ClientProfile {
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return profiles.Chrome_131
	}
	if p, ok := profiles.MappedTLSClients[key]; ok {
		return p
	}
	// aliases
	switch key {
	case "chrome", "chrome131", "chrome_131":
		return profiles.Chrome_131
	case "chrome133", "chrome_133":
		return profiles.Chrome_133
	default:
		return profiles.Chrome_131
	}
}

// Do executes req with Chrome TLS impersonation and returns a stdlib *http.Response.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c == nil {
		return nil, fmt.Errorf("browserhttp: nil client")
	}
	if err := c.ensure(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("browserhttp: nil request")
	}

	proxyURL := strings.TrimSpace(c.ProxyURL)
	if c.Proxy != nil {
		if u, err := c.Proxy(req); err == nil && u != nil {
			proxyURL = u.String()
		}
	}

	c.mu.Lock()
	if proxyURL != c.curPx {
		if err := c.inner.SetProxy(proxyURL); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("browserhttp: set proxy: %w", err)
		}
		c.curPx = proxyURL
	}
	// Build fhttp request under lock only for proxy swap; Do can run unlocked
	// after we copy headers — but SetProxy mutates shared client, so hold lock
	// for the whole Do to keep proxy sticky for this request.
	fReq, err := fhttp.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), req.Body)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	fReq.Header = fhttp.Header{}
	for k, vals := range req.Header {
		for _, v := range vals {
			fReq.Header.Add(k, v)
		}
	}
	// Header order hint for Chrome-like clients (fhttp uses order when present).
	if len(fReq.Header[fhttp.HeaderOrderKey]) == 0 {
		fReq.Header[fhttp.HeaderOrderKey] = []string{
			"accept",
			"accept-language",
			"authorization",
			"content-type",
			"cookie",
			"origin",
			"referer",
			"sec-ch-ua",
			"sec-ch-ua-mobile",
			"sec-ch-ua-platform",
			"sec-fetch-dest",
			"sec-fetch-mode",
			"sec-fetch-site",
			"user-agent",
			"x-cluster",
		}
	}
	resp, err := c.inner.Do(fReq)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return convertResponse(resp), nil
}

func convertResponse(r *fhttp.Response) *http.Response {
	if r == nil {
		return nil
	}
	h := make(http.Header, len(r.Header))
	for k, vals := range r.Header {
		// skip fhttp internal keys
		if strings.HasPrefix(k, "Pseudo-") || k == fhttp.HeaderOrderKey || k == fhttp.PHeaderOrderKey {
			continue
		}
		for _, v := range vals {
			h.Add(k, v)
		}
	}
	var body io.ReadCloser = http.NoBody
	if r.Body != nil {
		body = r.Body
	}
	return &http.Response{
		Status:           r.Status,
		StatusCode:       r.StatusCode,
		Proto:            r.Proto,
		ProtoMajor:       r.ProtoMajor,
		ProtoMinor:       r.ProtoMinor,
		Header:           h,
		Body:             body,
		ContentLength:    r.ContentLength,
		TransferEncoding: r.TransferEncoding,
		Close:            r.Close,
		Uncompressed:     r.Uncompressed,
		Trailer:          http.Header(r.Trailer),
		Request:          nil,
	}
}
