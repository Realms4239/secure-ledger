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

func TestValidateJWT_Hostile(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-for-hs256")
	other := []byte("different-secret-32-bytes-long!!")
	valid := func(claims jwt.MapClaims, key []byte) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	future := time.Now().Add(time.Hour).Unix()
	cases := map[string]string{
		"wrong secret": valid(jwt.MapClaims{"sub": "alice", "exp": future}, other),
		"future nbf": valid(jwt.MapClaims{
			"sub": "alice", "exp": future, "nbf": time.Now().Add(time.Hour).Unix(),
		}, secret),
		"missing sub": valid(jwt.MapClaims{"exp": future}, secret),
		"empty sub":   valid(jwt.MapClaims{"sub": "", "exp": future}, secret),
		"garbage":     "not.a.token",
		"empty":       "",
	}
	for name, tok := range cases {
		if _, err := ValidateJWT(tok, secret); err == nil {
			t.Fatalf("%s: want rejection", name)
		}
	}
	// none algorithm is never accepted regardless of claims
	noneTok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"sub": "alice"})
	unsigned, err := noneTok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateJWT(unsigned, secret); err == nil {
		t.Fatal("want rejection of alg=none")
	}
}
