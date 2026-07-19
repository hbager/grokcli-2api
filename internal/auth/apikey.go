package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
)

var ErrInvalidAPIKey = errors.New("invalid or missing API key")

type APIKeyRecord struct {
	ID           string
	Name         string
	Prefix       string
	KeyHash      string
	Enabled      bool
	RequestCount int64
	LastUsedAt   *time.Time
}

type apiKeyStore interface {
	HasEnabledAPIKeys(context.Context) (bool, error)
	FindAPIKeyByHash(context.Context, string) (*postgres.APIKeyRecord, error)
	TouchAPIKeyUsage(context.Context, string) error
}

type APIKeyVerifier struct {
	cfg             config.Config
	store           apiKeyStore
	usageTouchSlots chan struct{}
}

const maxConcurrentAPIKeyUsageTouches = 32

func NewAPIKeyVerifier(cfg config.Config, store *postgres.Connector) *APIKeyVerifier {
	if store == nil {
		return newAPIKeyVerifier(cfg, nil)
	}
	return newAPIKeyVerifier(cfg, store)
}

func newAPIKeyVerifier(cfg config.Config, store apiKeyStore) *APIKeyVerifier {
	return &APIKeyVerifier{
		cfg:             cfg,
		store:           store,
		usageTouchSlots: make(chan struct{}, maxConcurrentAPIKeyUsageTouches),
	}
}

func (v *APIKeyVerifier) Require(ctx context.Context, r *http.Request) (*APIKeyRecord, error) {
	token := tokenFromRequest(r)
	required, err := v.AuthRequired(ctx)
	if err != nil {
		return nil, err
	}
	if !required {
		if token == "" {
			return nil, nil
		}
		return v.Verify(ctx, token)
	}
	if token == "" {
		return nil, ErrInvalidAPIKey
	}
	rec, err := v.Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, ErrInvalidAPIKey
	}
	return rec, nil
}

func (v *APIKeyVerifier) AuthRequired(ctx context.Context) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v.cfg.RequireAPIKey)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	if strings.TrimSpace(v.cfg.LegacyAPIKey) != "" {
		return true, nil
	}
	if v.store == nil {
		return false, nil
	}
	return v.store.HasEnabledAPIKeys(ctx)
}

func (v *APIKeyVerifier) Verify(ctx context.Context, token string) (*APIKeyRecord, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}
	if legacy := strings.TrimSpace(v.cfg.LegacyAPIKey); legacy != "" && constantTimeEqual(token, legacy) {
		h := hashKey(token)
		return &APIKeyRecord{ID: "env", Name: "env:GROK2API_API_KEY", Prefix: prefix(token), KeyHash: h, Enabled: true}, nil
	}
	if v.store == nil {
		return nil, nil
	}
	h := hashKey(token)
	row, err := v.store.FindAPIKeyByHash(ctx, h)
	if err != nil || row == nil || !row.Enabled {
		return nil, err
	}
	rec := &APIKeyRecord{
		ID:           row.ID,
		Name:         row.Name,
		Prefix:       row.Prefix,
		KeyHash:      row.KeyHash,
		Enabled:      row.Enabled,
		RequestCount: row.RequestCount,
		LastUsedAt:   row.LastUsedAt,
	}
	v.touchAPIKeyUsage(rec.ID)
	return rec, nil
}

func (v *APIKeyVerifier) touchAPIKeyUsage(id string) {
	if v == nil || v.store == nil || id == "" || id == "env" {
		return
	}
	select {
	case v.usageTouchSlots <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-v.usageTouchSlots }()
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		_ = v.store.TouchAPIKeyUsage(ctx, id)
	}()
}

func tokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if authorization := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return strings.TrimSpace(authorization[7:])
	}
	return strings.TrimSpace(r.Header.Get("x-api-key"))
}

func hashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func prefix(raw string) string {
	if len(raw) >= 12 {
		return raw[:12]
	}
	return raw
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
