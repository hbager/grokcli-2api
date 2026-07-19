package postgres

import (
	"testing"
)

func TestCompactQuotaSnapshotExhausted(t *testing.T) {
	snap := compactQuotaSnapshot("acc-1", map[string]any{
		"ok":            true,
		"exhausted":     true,
		"auto_disabled": true,
		"used":          10.0,
		"monthly_limit": 10.0,
		"email":         "a@b.c",
		"display":       map[string]any{"summary": "$10.00 / $10.00"},
		"source":        "billing",
	})
	if snap["account_id"] != "acc-1" {
		t.Fatalf("account_id=%v", snap["account_id"])
	}
	if !truthyAny(snap["exhausted"]) {
		t.Fatal("expected exhausted")
	}
	if snap["summary"] != "$10.00 / $10.00" {
		t.Fatalf("summary=%v", snap["summary"])
	}
	if snap["source"] != "billing" {
		t.Fatalf("source=%v", snap["source"])
	}
}

func TestCompactQuotaSnapshotHealthy(t *testing.T) {
	snap := compactQuotaSnapshot("acc-2", map[string]any{
		"ok":            true,
		"exhausted":     false,
		"used":          1.0,
		"monthly_limit": 20.0,
		"fetched_at":    int64(123),
	})
	if !truthyAny(snap["ok"]) {
		t.Fatal("expected ok")
	}
	if truthyAny(snap["exhausted"]) {
		t.Fatal("expected not exhausted")
	}
	if snap["fetched_at"] != int64(123) {
		t.Fatalf("fetched_at=%v", snap["fetched_at"])
	}
}

func TestTruthyAny(t *testing.T) {
	if !truthyAny(true) || truthyAny(false) {
		t.Fatal("bool")
	}
	if !truthyAny("true") || !truthyAny("YES") || truthyAny("no") {
		t.Fatal("string")
	}
	if !truthyAny(1) || truthyAny(0) {
		t.Fatal("int")
	}
}

func TestFirstNonEmptyString(t *testing.T) {
	if got := firstNonEmptyString("", "  ", "x", "y"); got != "x" {
		t.Fatalf("got %q", got)
	}
	if got := firstNonEmptyString(); got != "" {
		t.Fatalf("empty got %q", got)
	}
}
