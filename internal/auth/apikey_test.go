package auth

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
)

type fakeAPIKeyStore struct {
	mu      sync.Mutex
	enabled bool
	records map[string]postgres.APIKeyRecord
}

func (s *fakeAPIKeyStore) HasEnabledAPIKeys(context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled, nil
}

func (s *fakeAPIKeyStore) FindAPIKeyByHash(_ context.Context, keyHash string) (*postgres.APIKeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[keyHash]
	if !ok {
		return nil, nil
	}
	copy := record
	return &copy, nil
}

func (s *fakeAPIKeyStore) TouchAPIKeyUsage(context.Context, string) error { return nil }

func (s *fakeAPIKeyStore) setEnabled(enabled bool) {
	s.mu.Lock()
	s.enabled = enabled
	s.mu.Unlock()
}

func (s *fakeAPIKeyStore) setRecord(keyHash string, record postgres.APIKeyRecord) {
	s.mu.Lock()
	s.records[keyHash] = record
	s.mu.Unlock()
}

type blockingTouchStore struct {
	fakeAPIKeyStore
	active  atomic.Int64
	maximum atomic.Int64
	release chan struct{}
}

func (s *blockingTouchStore) TouchAPIKeyUsage(ctx context.Context, _ string) error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maximum.Load()
		if active <= maximum || s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestTokenFromRequestPrecedence(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer preferred")
	req.Header.Set("x-api-key", "fallback")
	if got := tokenFromRequest(req); got != "preferred" {
		t.Fatalf("token = %q", got)
	}
}

func TestLegacyKeyVerification(t *testing.T) {
	verifier := NewAPIKeyVerifier(config.Config{LegacyAPIKey: "secret", RequireAPIKey: "auto"}, nil)
	rec, err := verifier.Verify(t.Context(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.ID != "env" || rec.Prefix != "secret" {
		t.Fatalf("unexpected record %#v", rec)
	}
	if rec, err := verifier.Verify(t.Context(), "wrong"); err != nil || rec != nil {
		t.Fatalf("expected wrong key miss, rec=%#v err=%v", rec, err)
	}
}

func TestAuthRequiredReflectsMutationFromAnotherInstance(t *testing.T) {
	store := &fakeAPIKeyStore{records: map[string]postgres.APIKeyRecord{}}
	instances := []*APIKeyVerifier{
		newAPIKeyVerifier(config.Config{RequireAPIKey: "auto"}, store),
		newAPIKeyVerifier(config.Config{RequireAPIKey: "auto"}, store),
	}
	for index, verifier := range instances {
		required, err := verifier.AuthRequired(t.Context())
		if err != nil || required {
			t.Fatalf("instance %d initial required=%v err=%v", index, required, err)
		}
	}
	store.setEnabled(true)
	for index, verifier := range instances {
		required, err := verifier.AuthRequired(t.Context())
		if err != nil || !required {
			t.Fatalf("instance %d required after shared mutation=%v err=%v", index, required, err)
		}
	}
}

func TestVerifyReflectsRevocationFromAnotherInstance(t *testing.T) {
	store := &fakeAPIKeyStore{enabled: true, records: map[string]postgres.APIKeyRecord{}}
	keyHash := hashKey("old-secret")
	store.setRecord(keyHash, postgres.APIKeyRecord{
		ID: "key-1", KeyHash: keyHash, Prefix: "old", Enabled: true,
	})
	instances := []*APIKeyVerifier{
		newAPIKeyVerifier(config.Config{RequireAPIKey: "auto"}, store),
		newAPIKeyVerifier(config.Config{RequireAPIKey: "auto"}, store),
	}
	for index, verifier := range instances {
		record, err := verifier.Verify(t.Context(), "old-secret")
		if err != nil || record == nil {
			t.Fatalf("instance %d initial record=%#v err=%v", index, record, err)
		}
	}
	store.setRecord(keyHash, postgres.APIKeyRecord{
		ID: "key-1", KeyHash: keyHash, Prefix: "old", Enabled: false,
	})
	for index, verifier := range instances {
		record, err := verifier.Verify(t.Context(), "old-secret")
		if err != nil || record != nil {
			t.Fatalf("instance %d record after shared revocation=%#v err=%v", index, record, err)
		}
	}
}

func TestVerifyBoundsConcurrentUsageTouches(t *testing.T) {
	keyHash := hashKey("secret")
	store := &blockingTouchStore{
		fakeAPIKeyStore: fakeAPIKeyStore{
			enabled: true,
			records: map[string]postgres.APIKeyRecord{
				keyHash: {ID: "key-1", KeyHash: keyHash, Prefix: "secret", Enabled: true},
			},
		},
		release: make(chan struct{}),
	}
	defer close(store.release)
	verifier := newAPIKeyVerifier(config.Config{RequireAPIKey: "true"}, store)

	for range 1000 {
		record, err := verifier.Verify(t.Context(), "secret")
		if err != nil || record == nil {
			t.Fatalf("record=%#v err=%v", record, err)
		}
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for store.maximum.Load() < maxConcurrentAPIKeyUsageTouches && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if maximum := store.maximum.Load(); maximum > maxConcurrentAPIKeyUsageTouches {
		t.Fatalf("concurrent usage touches=%d want <=%d", maximum, maxConcurrentAPIKeyUsageTouches)
	}
}

func TestAuthRequiredModes(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want bool
	}{
		{"true", true},
		{"1", true},
		{"off", false},
		{"false", false},
	} {
		verifier := NewAPIKeyVerifier(config.Config{RequireAPIKey: tc.mode}, nil)
		got, err := verifier.AuthRequired(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("mode %s got %v want %v", tc.mode, got, tc.want)
		}
	}
}
