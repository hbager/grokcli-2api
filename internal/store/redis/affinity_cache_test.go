package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAffinityLocalCacheRoundTrip(t *testing.T) {
	affinityCacheMu.Lock()
	affinityCache = map[string]affinityCacheEntry{}
	responseCache = map[string]responseCacheEntry{}
	affinityCacheMu.Unlock()

	if _, ok := affinityCacheGet("fp-1"); ok {
		t.Fatal("expected miss")
	}
	affinityCacheSet("fp-1", "acc-1")
	id, ok := affinityCacheGet("fp-1")
	if !ok || id != "acc-1" {
		t.Fatalf("got %q ok=%v", id, ok)
	}
	// Expired entry should miss.
	affinityCacheMu.Lock()
	e := affinityCache["fp-1"]
	e.expires = time.Now().Add(-time.Second)
	affinityCache["fp-1"] = e
	affinityCacheMu.Unlock()
	if _, ok := affinityCacheGet("fp-1"); ok {
		t.Fatal("expected expired miss")
	}
}

func TestResponseLocalCacheRoundTrip(t *testing.T) {
	affinityCacheMu.Lock()
	responseCache = map[string]responseCacheEntry{}
	affinityCacheMu.Unlock()

	responseCacheSet("resp_1", "acc-9", "pck-root")
	acc, pck, ok := responseCacheGet("resp_1")
	if !ok || acc != "acc-9" || pck != "pck-root" {
		t.Fatalf("got %q %q ok=%v", acc, pck, ok)
	}
}

func TestResponseLocalCacheEnforcesLimitAndEvictsOldest(t *testing.T) {
	affinityCacheMu.Lock()
	responseCache = map[string]responseCacheEntry{}
	affinityCacheMu.Unlock()

	for index := 0; index <= 8192; index++ {
		responseCacheSet(fmt.Sprintf("resp-%05d", index), "account", "")
	}
	affinityCacheMu.Lock()
	defer affinityCacheMu.Unlock()
	if len(responseCache) > 8192 {
		t.Fatalf("response cache size=%d", len(responseCache))
	}
	if _, exists := responseCache["resp-00000"]; exists {
		t.Fatal("oldest response cache entry was not evicted")
	}
	if _, exists := responseCache["resp-08192"]; !exists {
		t.Fatal("newest response cache entry was evicted")
	}
}

func TestScheduleWriteDropsCompletedFlushingKeys(t *testing.T) {
	affinityPendingMu.Lock()
	affinityPending = map[string]affinityPendingWrite{}
	affinityFlushing = map[string]bool{}
	affinityPendingMu.Unlock()
	responsePendingMu.Lock()
	responsePending = map[string]responsePendingWrite{}
	responseFlushing = map[string]bool{}
	responsePendingMu.Unlock()

	for index := 0; index < 100; index++ {
		key := fmt.Sprintf("key-%d", index)
		affinityScheduleWrite(key, "account", time.Minute, "", "", func(context.Context, string, string, time.Duration, string, string) error {
			return nil
		})
		responseScheduleWrite(key, "account", "", time.Minute, func(context.Context, string, string, string, time.Duration) error {
			return nil
		})
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		affinityPendingMu.Lock()
		affinityPendingCount, affinityFlushingCount := len(affinityPending), len(affinityFlushing)
		affinityPendingMu.Unlock()
		responsePendingMu.Lock()
		responsePendingCount, responseFlushingCount := len(responsePending), len(responseFlushing)
		responsePendingMu.Unlock()
		if affinityPendingCount == 0 && affinityFlushingCount == 0 && responsePendingCount == 0 && responseFlushingCount == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	affinityPendingMu.Lock()
	affinityPendingCount, affinityFlushingCount := len(affinityPending), len(affinityFlushing)
	affinityPendingMu.Unlock()
	responsePendingMu.Lock()
	responsePendingCount, responseFlushingCount := len(responsePending), len(responseFlushing)
	responsePendingMu.Unlock()
	t.Fatalf("affinity pending/flushing=%d/%d response=%d/%d", affinityPendingCount, affinityFlushingCount, responsePendingCount, responseFlushingCount)
}

func TestAffinityScheduleWriteCoalesces(t *testing.T) {
	// Reset pending maps
	affinityPendingMu.Lock()
	affinityPending = map[string]affinityPendingWrite{}
	affinityFlushing = map[string]bool{}
	affinityPendingMu.Unlock()

	var mu sync.Mutex
	var calls []string
	var accounts []string
	writer := func(ctx context.Context, fingerprint, accountID string, ttl time.Duration, sessionFP, promptCacheKey string) error {
		mu.Lock()
		calls = append(calls, fingerprint)
		accounts = append(accounts, accountID)
		mu.Unlock()
		// slow enough that subsequent schedules merge into pending
		time.Sleep(30 * time.Millisecond)
		return nil
	}

	// Burst: same key, three account updates — should flush latest, not 3 full serial if merged.
	affinityScheduleWrite("fp-coalesce", "a1", time.Hour, "", "", writer)
	affinityScheduleWrite("fp-coalesce", "a2", time.Hour, "", "", writer)
	affinityScheduleWrite("fp-coalesce", "a3", time.Hour, "", "", writer)

	// Wait for flush loop to drain
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		affinityPendingMu.Lock()
		pending := len(affinityPending)
		flushing := affinityFlushing["fp-coalesce"]
		affinityPendingMu.Unlock()
		if pending == 0 && !flushing {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) == 0 {
		t.Fatal("expected at least one redis write")
	}
	// Last write must be a3 (latest pending wins).
	if accounts[len(accounts)-1] != "a3" {
		t.Fatalf("last account=%v calls=%v", accounts, calls)
	}
	// Should be heavily coalesced (not 3+ independent starts without merge).
	if len(calls) > 3 {
		t.Fatalf("too many writes without coalesce: %d", len(calls))
	}
}
