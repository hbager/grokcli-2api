package redis

import (
	"context"
	"strings"
	"time"
)

type pickBackend interface {
	GetInflight(context.Context, string) (int64, error)
	GetInflightMany(context.Context, []string) map[string]int64
	MarkInflight(context.Context, string, int) (int64, error)
	MarkSoftUsed(context.Context, string, int, time.Time) (float64, error)
	ReleaseInflight(context.Context, string) error
}

type pickPenaltyBackend interface {
	GetSoftUsedAgeSec(context.Context, string, time.Time) (float64, error)
	GetSoftUsedAgeSecMany(context.Context, []string, time.Time) map[string]float64
}

const (
	pickObserverStripes   = 32
	pickObserverQueueSize = 64
)

type pickOperation struct {
	accountID string
	mark      bool
}

type pickObserverState struct {
	backend pickBackend
	queues  []chan pickOperation
}

type PickObserver struct {
	backend pickBackend
	state   *pickObserverState
}

func NewPickObserver(client *Client) PickObserver {
	if client == nil {
		return PickObserver{}
	}
	return newPickObserver(client)
}

func newPickObserver(backend pickBackend) PickObserver {
	if backend == nil {
		return PickObserver{}
	}
	state := &pickObserverState{
		backend: backend,
		queues:  make([]chan pickOperation, pickObserverStripes),
	}
	for index := range state.queues {
		state.queues[index] = make(chan pickOperation, pickObserverQueueSize)
		go state.run(state.queues[index])
	}
	return PickObserver{backend: backend, state: state}
}

func (o PickObserver) LoadPenalty(ctx context.Context, accountID string) int64 {
	if o.backend == nil {
		return 0
	}
	var penalty int64
	if inflight, err := o.backend.GetInflight(ctx, accountID); err == nil && inflight > 0 {
		penalty += inflightPenalty(inflight)
	}
	if penaltyBackend, ok := o.backend.(pickPenaltyBackend); ok {
		if age, err := penaltyBackend.GetSoftUsedAgeSec(ctx, accountID, time.Now()); err == nil {
			penalty += softUsedPenalty(age)
		}
	}
	return penalty
}

// LoadPenalties batches inflight and soft-used lookups for a candidate window (hot path).
func (o PickObserver) LoadPenalties(ctx context.Context, accountIDs []string) map[string]int64 {
	out := map[string]int64{}
	if o.backend == nil || len(accountIDs) == 0 {
		return out
	}
	inflight := o.backend.GetInflightMany(ctx, accountIDs)
	softUsed := map[string]float64{}
	if penaltyBackend, ok := o.backend.(pickPenaltyBackend); ok {
		softUsed = penaltyBackend.GetSoftUsedAgeSecMany(ctx, accountIDs, time.Now())
	}
	for _, id := range accountIDs {
		penalty := inflightPenalty(inflight[id])
		if age, ok := softUsed[id]; ok {
			penalty += softUsedPenalty(age)
		}
		if penalty > 0 {
			out[id] = penalty
		}
	}
	return out
}

func inflightPenalty(inflight int64) int64 {
	if inflight <= 0 {
		return 0
	}
	return inflight * (inflight + 1) / 2 * 1000
}

func softUsedPenalty(ageSec float64) int64 {
	if !(ageSec >= 0 && ageSec < 30) {
		return 0
	}
	return int64((30 - ageSec) * (800.0 / 30.0))
}

func (o PickObserver) MarkPick(_ context.Context, accountID string) {
	o.enqueue(pickOperation{accountID: accountID, mark: true})
}

func (o PickObserver) ReleasePick(_ context.Context, accountID string) {
	o.enqueue(pickOperation{accountID: accountID})
}

func (o PickObserver) enqueue(operation pickOperation) {
	operation.accountID = strings.TrimSpace(operation.accountID)
	if o.state == nil || operation.accountID == "" {
		return
	}
	o.state.queues[pickStripe(operation.accountID)] <- operation
}

func (s *pickObserverState) run(queue <-chan pickOperation) {
	for operation := range queue {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		if operation.mark {
			_, _ = s.backend.MarkInflight(ctx, operation.accountID, InflightTTLSeconds)
			_, _ = s.backend.MarkSoftUsed(ctx, operation.accountID, SoftUsedTTLSeconds, time.Now())
		} else {
			_ = s.backend.ReleaseInflight(ctx, operation.accountID)
		}
		cancel()
	}
}

func pickStripe(accountID string) int {
	hash := uint32(2166136261)
	for _, value := range []byte(accountID) {
		hash ^= uint32(value)
		hash *= 16777619
	}
	return int(hash % pickObserverStripes)
}
