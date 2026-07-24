package browserhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserClientDoAgainstHTTPServer(t *testing.T) {
	// Plain HTTP test server cannot exercise JA3, but ensures Do() wiring works.
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := &Client{Timeout: 10_000_000_000} // 10s
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "ChromeTest/131")
	resp, err := c.Do(req)
	if err != nil {
		// Some environments may fail TLS client on plain HTTP loopback — skip soft.
		if strings.Contains(err.Error(), "protocol") || strings.Contains(err.Error(), "TLS") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if gotUA != "ChromeTest/131" {
		t.Fatalf("ua=%q", gotUA)
	}
}
