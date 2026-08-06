package server

import (
	"net/http"
	"testing"
)

func TestShouldDisableSSOSurface(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		errText string
		want    bool
	}{
		{
			name:    "DPoP protocol rejection",
			status:  http.StatusForbidden,
			errText: `{"code":"unauthorized:dpop-required","message":"DPoP proof required but was not verified"}`,
			want:    false,
		},
		{
			name:    "invalid SSO session",
			status:  http.StatusUnauthorized,
			errText: "Failed to look up session ID",
			want:    true,
		},
		{
			name:    "generic forbidden",
			status:  http.StatusForbidden,
			errText: "Forbidden",
			want:    true,
		},
		{
			name:    "non-authentication error",
			status:  http.StatusBadGateway,
			errText: "Forbidden",
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldDisableSSOSurface(test.status, test.errText); got != test.want {
				t.Fatalf("shouldDisableSSOSurface(%d, %q) = %t, want %t", test.status, test.errText, got, test.want)
			}
		})
	}
}
