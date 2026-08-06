package console

import "testing"

func TestIsDPoPProtocolRejection(t *testing.T) {
	tests := []struct {
		name    string
		errText string
		want    bool
	}{
		{
			name:    "code",
			errText: "unauthorized:dpop-required",
			want:    true,
		},
		{
			name:    "JSON code",
			errText: `{"error":{"code":"unauthorized:dpop-required"}}`,
			want:    true,
		},
		{
			name:    "JSON message",
			errText: `{"code":"unauthorized","message":"DPoP proof required but was not verified"}`,
			want:    true,
		},
		{
			name:    "message in text",
			errText: "request rejected: DPoP proof required but was not verified",
			want:    true,
		},
		{
			name:    "invalid session",
			errText: "Failed to look up session ID",
		},
		{
			name:    "generic forbidden",
			errText: `{"error":{"code":"forbidden","message":"Forbidden"}}`,
		},
		{
			name:    "arbitrary dpop mention",
			errText: "generic DPoP request failed",
		},
		{
			name:    "unrelated JSON field",
			errText: `{"documentation":"unauthorized:dpop-required","message":"Forbidden"}`,
		},
		{
			name:    "marker embedded outside recognized JSON field",
			errText: `{"documentation":"DPoP proof required but was not verified","message":"Forbidden"}`,
		},
		{
			name:    "code with extra suffix",
			errText: "unauthorized:dpop-required-later",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsDPoPProtocolRejection(test.errText); got != test.want {
				t.Fatalf("IsDPoPProtocolRejection(%q) = %t, want %t", test.errText, got, test.want)
			}
		})
	}
}
