package postgres

import "testing"

func TestPoolCandidateWindowSupportsMaximumRetryChain(t *testing.T) {
	if poolCandidateWindow < 64 {
		t.Fatalf("pool candidate window=%d want at least 64", poolCandidateWindow)
	}
}

func TestEffectiveUpstreamRetryCount(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]any
		want   int
	}{
		{name: "new zero wins", values: map[string]any{"upstream_retry_count": float64(0), "max_failover_attempts": float64(64)}, want: 0},
		{name: "new value wins", values: map[string]any{"upstream_retry_count": float64(5), "max_failover_attempts": float64(2)}, want: 5},
		{name: "legacy total converts", values: map[string]any{"max_failover_attempts": float64(4)}, want: 3},
		{name: "legacy minimum converts", values: map[string]any{"max_failover_attempts": float64(1)}, want: 0},
		{name: "missing uses default", values: map[string]any{}, want: 3},
		{name: "new value clamps high", values: map[string]any{"upstream_retry_count": float64(100)}, want: 63},
		{name: "legacy value clamps high", values: map[string]any{"max_failover_attempts": float64(100)}, want: 63},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveUpstreamRetryCount(tt.values); got != tt.want {
				t.Fatalf("effectiveUpstreamRetryCount(%v)=%d want %d", tt.values, got, tt.want)
			}
		})
	}
}
