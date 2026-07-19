package redis

import (
	"context"
	"sync"
	"testing"
	"time"
)

type orderedPickBackend struct {
	markGate      chan struct{}
	releaseCalled chan struct{}
	releaseOnce   sync.Once
	mu            sync.Mutex
	inflight      int64
}

func (b *orderedPickBackend) GetInflight(context.Context, string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inflight, nil
}

func (b *orderedPickBackend) GetInflightMany(context.Context, []string) map[string]int64 {
	return map[string]int64{}
}

func (b *orderedPickBackend) MarkInflight(context.Context, string, int) (int64, error) {
	<-b.markGate
	b.mu.Lock()
	b.inflight++
	value := b.inflight
	b.mu.Unlock()
	return value, nil
}

func (b *orderedPickBackend) MarkSoftUsed(context.Context, string, int, time.Time) (float64, error) {
	return 0, nil
}

func (b *orderedPickBackend) ReleaseInflight(context.Context, string) error {
	b.mu.Lock()
	b.inflight--
	b.mu.Unlock()
	b.releaseOnce.Do(func() { close(b.releaseCalled) })
	return nil
}

func TestPickObserverPreservesMarkReleaseOrder(t *testing.T) {
	backend := &orderedPickBackend{
		markGate:      make(chan struct{}),
		releaseCalled: make(chan struct{}),
	}
	observer := newPickObserver(backend)
	observer.MarkPick(t.Context(), "account-1")
	observer.ReleasePick(t.Context(), "account-1")

	select {
	case <-backend.releaseCalled:
		close(backend.markGate)
		t.Fatal("release overtook blocked mark")
	case <-time.After(25 * time.Millisecond):
	}
	close(backend.markGate)
	select {
	case <-backend.releaseCalled:
	case <-time.After(time.Second):
		t.Fatal("ordered release did not run")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		inflight, _ := backend.GetInflight(t.Context(), "account-1")
		if inflight == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	inflight, _ := backend.GetInflight(t.Context(), "account-1")
	t.Fatalf("inflight=%d want 0", inflight)
}
