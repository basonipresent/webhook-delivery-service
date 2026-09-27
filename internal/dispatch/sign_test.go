package dispatch

import "testing"

func TestSignKnownVector(t *testing.T) {
	// Computed independently:
	// printf '%s' '1700000000.{"event_id":"evt_1"}' | openssl dgst -sha256 -hmac 'whsec_test' -hex
	const want = "0344289cae2f5eb6d2640d79205fa65017efc017e224155b7ef95f784061172c"
	got := Sign([]byte("whsec_test"), 1700000000, []byte(`{"event_id":"evt_1"}`))
	if got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
}

func TestSignDependsOnEveryInput(t *testing.T) {
	base := Sign([]byte("secret"), 100, []byte("body"))
	tests := []struct {
		name   string
		secret string
		ts     int64
		body   string
	}{
		{"secret", "secret2", 100, "body"},
		{"timestamp", "secret", 101, "body"},
		{"body", "secret", 100, "body "},
		// The "." separator keeps ts/body boundaries unambiguous: ts=10, body="0body"
		// signs "10.0body", not the same bytes as ts=100, body="body".
		{"boundary shift", "secret", 10, "0body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sign([]byte(tt.secret), tt.ts, []byte(tt.body)); got == base {
				t.Fatalf("changing %s did not change the signature", tt.name)
			}
		})
	}
}
