package authn

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestValidateJWT_ValidAndExpired(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-for-hs256")
	// valid token
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "alice", "exp": time.Now().Add(time.Hour).Unix(),
		"nbf": time.Now().Add(-time.Minute).Unix(),
	})
	s, _ := tok.SignedString(secret)
	sub, err := ValidateJWT(s, secret)
	if err != nil || sub != "alice" {
		t.Fatalf("want alice, got %q err %v", sub, err)
	}

	// expired
	tok2 := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "alice", "exp": time.Now().Add(-time.Hour).Unix(),
	})
	s2, _ := tok2.SignedString(secret)
	if _, err := ValidateJWT(s2, secret); err == nil {
		t.Fatal("want err for expired token")
	}
}
