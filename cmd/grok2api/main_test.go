package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresDynamicOutboundProxyToAllAccountTraffic(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"proxySelector := outboundproxy.New(runtimeCfg.Load)",
		"HTTP: grok.NewHTTPClient(proxySelector.Proxy)",
		"oidcTransport.Proxy = proxySelector.Proxy",
		"healthSvc.SetProxy(proxySelector.Proxy)",
		"quotaSvc.SetProxy(proxySelector.Proxy)",
		"Quota:             quotaSvc",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("outbound proxy wiring missing %q", required)
		}
	}
}
