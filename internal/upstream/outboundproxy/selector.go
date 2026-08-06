package outboundproxy

import (
	"context"
	"errors"
	"hash/fnv"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/hm2899/grokcli-2api/internal/config"
)

type accountIDKey struct{}

type pinnedProxyKey struct{}

type pinnedProxy struct {
	url *url.URL
}

func WithAccountID(ctx context.Context, accountID string) context.Context {
	if strings.TrimSpace(accountID) == "" {
		return ctx
	}
	return context.WithValue(ctx, accountIDKey{}, strings.TrimSpace(accountID))
}

func accountID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(accountIDKey{}).(string)
	return strings.TrimSpace(value)
}

// WithPinnedProxy binds one proxy selection to ctx. A nil proxy is an
// explicit direct connection, distinct from the absence of a binding.
func WithPinnedProxy(ctx context.Context, proxyURL *url.URL) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, pinnedProxyKey{}, pinnedProxy{url: cloneURL(proxyURL)})
}

// PinnedProxy returns the proxy bound by WithPinnedProxy. The bool reports
// whether a binding exists, including an explicit direct (nil) binding.
func PinnedProxy(ctx context.Context) (*url.URL, bool) {
	if ctx == nil {
		return nil, false
	}
	binding, ok := ctx.Value(pinnedProxyKey{}).(pinnedProxy)
	if !ok {
		return nil, false
	}
	return cloneURL(binding.url), true
}

func cloneURL(value *url.URL) *url.URL {
	if value == nil {
		return nil
	}
	clone := *value
	if value.User != nil {
		user := *value.User
		clone.User = &user
	}
	return &clone
}

type Selector struct {
	load func() config.Config
	next atomic.Uint64
}

func New(load func() config.Config) *Selector {
	if load == nil {
		load = func() config.Config { return config.Config{} }
	}
	return &Selector{load: load}
}

func (s *Selector) Proxy(request *http.Request) (*url.URL, error) {
	if s == nil || request == nil {
		return nil, nil
	}
	if proxyURL, ok := PinnedProxy(request.Context()); ok {
		return proxyURL, nil
	}
	cfg := s.load()
	if !cfg.OutboundProxyConfigured {
		return http.ProxyFromEnvironment(request)
	}
	if !cfg.OutboundProxyEnabled {
		return nil, nil
	}
	text := strings.TrimSpace(cfg.OutboundProxy)
	if text == "" {
		return nil, nil
	}
	pool := parsePool(text, cfg.OutboundProxyUsername, cfg.OutboundProxyPassword)
	if len(pool) == 0 {
		return nil, errors.New("outbound proxy is enabled but has no valid entries")
	}
	if len(pool) == 1 {
		return pool[0], nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.OutboundProxyStrategy)) {
	case "sticky":
		return pool[0], nil
	case "random":
		return pool[rand.IntN(len(pool))], nil
	default:
		if id := accountID(request.Context()); id != "" {
			hash := fnv.New32a()
			_, _ = hash.Write([]byte(id))
			return pool[int(hash.Sum32()%uint32(len(pool)))], nil
		}
		index := s.next.Add(1) - 1
		return pool[int(index%uint64(len(pool)))], nil
	}
}

func parsePool(text, username, password string) []*url.URL {
	entries := strings.FieldsFunc(text, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ',' || r == ';'
	})
	pool := make([]*url.URL, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		proxyURL, err := parseProxy(entry)
		if err != nil {
			continue
		}
		if proxyURL.User == nil && strings.TrimSpace(username) != "" {
			if password != "" {
				proxyURL.User = url.UserPassword(strings.TrimSpace(username), password)
			} else {
				proxyURL.User = url.User(strings.TrimSpace(username))
			}
		}
		pool = append(pool, proxyURL)
	}
	return pool
}

func parseProxy(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("empty proxy")
	}
	scheme := "http"
	rest := raw
	if index := strings.Index(raw, "://"); index >= 0 {
		scheme = strings.ToLower(strings.TrimSpace(raw[:index]))
		rest = raw[index+3:]
	}
	if !strings.Contains(rest, "@") {
		parts := strings.Split(rest, ":")
		if len(parts) == 4 && parts[0] != "" && parts[1] != "" && parts[2] != "" {
			return &url.URL{
				Scheme: scheme,
				Host:   net.JoinHostPort(parts[0], parts[1]),
				User:   url.UserPassword(parts[2], parts[3]),
			}, nil
		}
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return nil, errors.New("invalid proxy")
	}
	return parsed, nil
}
