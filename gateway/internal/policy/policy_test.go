package policy

import "testing"

func TestAuthorized(t *testing.T) {
	if !Authorized("alice", "alice") {
		t.Fatal("alice should authorize alice")
	}
	if Authorized("bob", "alice") {
		t.Fatal("bob should not authorize alice")
	}
}
