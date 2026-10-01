package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"secureledger/gateway/internal/audit"
	"secureledger/gateway/internal/idempotency"
	"secureledger/gateway/internal/ipc"
)

const testSecret = "test-secret-32-bytes-long-for-hs256"

type stubStatus struct{}

func (stubStatus) GetSagaStatus(_ context.Context, sagaID string) (string, []string, string, error) {
	return "InProgress", []string{"debit"}, time.Now().UTC().Format(time.RFC3339Nano), nil
}

type errStatus struct{}

func (errStatus) GetSagaStatus(context.Context, string) (string, []string, string, error) {
	return "", nil, "", errors.New("not found")
}

func makeToken(t *testing.T, secret []byte, sub string, exp time.Time) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub, "exp": exp.Unix(), "nbf": time.Now().Add(-time.Minute).Unix(),
	})
	s, err := tok.SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type deps struct {
	h     *TransfersHandler
	ring  *ipc.RingWriter
	audit *bytes.Buffer
	mux   *http.ServeMux
}

func setup(t *testing.T) *deps {
	t.Helper()
	secret := []byte(testSecret)
	ring, err := ipc.NewRingWriter(filepath.Join(t.TempDir(), "test.ring"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ring.Close() })
	buf := &bytes.Buffer{}
	h := NewTransfersHandler(secret, idempotency.New(), ring, audit.New(buf), stubStatus{})
	mux := http.NewServeMux()
	h.Register(mux)
	return &deps{h: h, ring: ring, audit: buf, mux: mux}
}

func (d *deps) do(t *testing.T, method, target, token, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	d.mux.ServeHTTP(rec, req)
	return rec
}

func validBody() string { return `{"from_account":"alice","to_account":"bob","amount":100}` }

func TestTransferPipeline_Table(t *testing.T) {
	secret := []byte(testSecret)
	validTok := makeToken(t, secret, "alice", time.Now().Add(time.Hour))
	expiredTok := makeToken(t, secret, "alice", time.Now().Add(-time.Hour))
	bobTok := makeToken(t, secret, "bob", time.Now().Add(time.Hour))

	tests := []struct {
		name       string
		token      string
		key        string
		body       string
		preSeed    func(d *deps) // runs before request
		wantCode   int
		wantAudit  string // decision substring expected in audit buffer
		checkExtra func(t *testing.T, d *deps, rec *httptest.ResponseRecorder)
	}{
		{
			name: "missing bearer", token: "", key: uuid.NewString(), body: validBody(),
			wantCode: http.StatusUnauthorized, wantAudit: `"decision":"unauthorized"`,
			checkExtra: func(t *testing.T, d *deps, _ *httptest.ResponseRecorder) {
				if got := d.h.unauthorizedTotal.Load(); got != 1 {
					t.Fatalf("unauthorized_total = %d, want 1", got)
				}
			},
		},
		{
			name: "expired token", token: expiredTok, key: uuid.NewString(), body: validBody(),
			wantCode: http.StatusUnauthorized, wantAudit: `"decision":"unauthorized"`,
		},
		{
			name: "missing idempotency key", token: validTok, key: "", body: validBody(),
			wantCode: http.StatusBadRequest, wantAudit: `"decision":"bad_request"`,
		},
		{
			name: "non-uuid key", token: validTok, key: "not-a-uuid", body: validBody(),
			wantCode: http.StatusBadRequest, wantAudit: `"decision":"bad_request"`,
		},
		{
			name: "malformed body", token: validTok, key: uuid.NewString(), body: `{bad json`,
			wantCode: http.StatusBadRequest, wantAudit: `"decision":"bad_request"`,
		},
		{
			name: "zero amount", token: validTok, key: uuid.NewString(),
			body: `{"from_account":"alice","to_account":"bob","amount":0}`,
			wantCode: http.StatusBadRequest, wantAudit: `"decision":"bad_request"`,
		},
		{
			name: "negative amount", token: validTok, key: uuid.NewString(),
			body: `{"from_account":"alice","to_account":"bob","amount":-5}`,
			wantCode: http.StatusBadRequest, wantAudit: `"decision":"bad_request"`,
		},
		{
			name: "policy violation", token: bobTok, key: uuid.NewString(), body: validBody(),
			wantCode: http.StatusForbidden, wantAudit: `"decision":"forbidden"`,
		},
		{
			name: "duplicate key returns original saga_id", token: validTok,
			key: "11111111-2222-3333-4444-555555555555", body: validBody(),
			preSeed: func(d *deps) {
				d.h.idem.CheckOrStore("11111111-2222-3333-4444-555555555555", "existing-saga-id")
			},
			wantCode: http.StatusConflict, wantAudit: `"decision":"duplicate"`,
			checkExtra: func(t *testing.T, d *deps, rec *httptest.ResponseRecorder) {
				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if resp["saga_id"] != "existing-saga-id" {
					t.Fatalf("saga_id = %q, want existing-saga-id", resp["saga_id"])
				}
				if d.h.duplicateKeyTotal.Load() != 1 {
					t.Fatal("duplicate_key_total = 0, want 1")
				}
			},
		},
		{
			name: "success accepted", token: validTok, key: uuid.NewString(), body: validBody(),
			wantCode: http.StatusAccepted, wantAudit: `"decision":"accepted"`,
			checkExtra: func(t *testing.T, d *deps, rec *httptest.ResponseRecorder) {
				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				sagaID := resp["saga_id"]
				if sagaID == "" || resp["status"] != "accepted" {
					t.Fatalf("resp = %v, want saga_id+accepted", resp)
				}
				typ, gotID, payload, err := ipc.UnmarshalSlot(mustSlot(t, d.ring))
				if err != nil || typ != EventTypeSagaStart {
					t.Fatalf("slot read: typ=%x err=%v", typ, err)
				}
				id, err := uuid.Parse(sagaID)
				if err != nil {
					t.Fatal(err)
				}
				if [16]byte(id) != gotID {
					t.Fatal("ring saga_id mismatch")
				}
				var p map[string]any
				if err := json.Unmarshal(payload, &p); err != nil {
					t.Fatal(err)
				}
				for _, k := range []string{"from", "to", "amount", "idempotency_key"} {
					if _, ok := p[k]; !ok {
						t.Fatalf("payload missing %s: %s", k, payload)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := setup(t)
			if tt.preSeed != nil {
				tt.preSeed(d)
			}
			rec := d.do(t, "POST", "/transfers", tt.token, tt.key, tt.body)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if !strings.Contains(d.audit.String(), tt.wantAudit) {
				t.Fatalf("audit buf %q missing %s", d.audit.String(), tt.wantAudit)
			}
			if tt.checkExtra != nil {
				tt.checkExtra(t, d, rec)
			}
		})
	}
}

func TestTransferRejectsFractionalAmount(t *testing.T) {	d := setup(t)
	tok := makeToken(t, []byte(testSecret), "alice", time.Now().Add(time.Hour))
	rec := d.do(t, "POST", "/transfers", tok, uuid.NewString(), `{"from_account":"alice","to_account":"bob","amount":100.5}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(d.audit.String(), `"decision":"bad_request"`) {
		t.Fatalf("audit missing bad_request: %q", d.audit.String())
	}
}

func TestTransferPayloadCarriesOperatorTxid(t *testing.T) {
	d := setup(t)
	tok := makeToken(t, []byte(testSecret), "alice", time.Now().Add(time.Hour))
	body := `{"from_account":"alice","to_account":"bob","amount":100,"operator":"orange","txid":"op-tx-1"}`
	rec := d.do(t, "POST", "/transfers", tok, uuid.NewString(), body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	_, _, payload, err := ipc.UnmarshalSlot(mustSlot(t, d.ring))
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatal(err)
	}
	if p["operator"] != "orange" || p["txid"] != "op-tx-1" {
		t.Fatalf("payload operator/txid not carried: %s", payload)
	}
}

func mustSlot(t *testing.T, rw *ipc.RingWriter) []byte {
	t.Helper()
	slot, err := rw.ReadRaw(0)
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

func TestTransferRingFull503(t *testing.T) {
	secret := []byte(testSecret)
	ring, err := ipc.NewRingWriter(filepath.Join(t.TempDir(), "full.ring"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	var id [16]byte
	for i := 0; i < 4; i++ {
		if err := ring.Write(EventTypeSagaStart, id, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	buf := &bytes.Buffer{}
	h := NewTransfersHandler(secret, idempotency.New(), ring, audit.New(buf), stubStatus{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST", "/transfers", strings.NewReader(validBody()))
	req.Header.Set("Authorization", "Bearer "+makeToken(t, secret, "alice", time.Now().Add(time.Hour)))
	req.Header.Set("Idempotency-Key", uuid.NewString())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(buf.String(), `"decision":"ring_full"`) {
		t.Fatalf("audit missing ring_full: %q", buf.String())
	}
	if h.ringFullTotal.Load() != 1 {
		t.Fatal("ring_full_total = 0, want 1")
	}
	metricsz := httptest.NewRequest("GET", "/metricsz", nil)
	mrec := httptest.NewRecorder()
	mux.ServeHTTP(mrec, metricsz)
	if !strings.Contains(mrec.Body.String(), "ring_full_total 1") {
		t.Fatalf("metricsz = %q", mrec.Body.String())
	}
}

func TestGetSagaStatus(t *testing.T) {
	newDeps := func(st StatusClient) (*bytes.Buffer, *http.ServeMux) {
		ring, err := ipc.NewRingWriter(filepath.Join(t.TempDir(), "s.ring"), 4)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ring.Close() })
		buf := &bytes.Buffer{}
		h := NewTransfersHandler([]byte(testSecret), idempotency.New(), ring, audit.New(buf), st)
		mux := http.NewServeMux()
		h.Register(mux)
		return buf, mux
	}

	t.Run("found", func(t *testing.T) {
		_, mux := newDeps(stubStatus{})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/sagas/abc-123", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("code = %d, want 200", rec.Code)
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp["state"] != "InProgress" {
			t.Fatalf("state = %v, want InProgress", resp["state"])
		}
	})

	t.Run("unknown not found", func(t *testing.T) {
		buf, mux := newDeps(errStatus{})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/sagas/nope", nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("code = %d, want 404", rec.Code)
		}
		if !strings.Contains(buf.String(), `"decision":"not_found"`) {
			t.Fatalf("audit missing not_found: %q", buf.String())
		}
	})
}

func TestMetricszCounters(t *testing.T) {
	d := setup(t)
	d.h.ringFullTotal.Add(2)
	d.h.unauthorizedTotal.Add(3)
	d.h.duplicateKeyTotal.Add(5)
	rec := httptest.NewRecorder()
	d.mux.ServeHTTP(rec, httptest.NewRequest("GET", "/metricsz", nil))
	want := "ring_full_total 2\nunauthorized_total 3\nduplicate_key_total 5\n"
	if rec.Body.String() != want {
		t.Fatalf("metricsz = %q, want %q", rec.Body.String(), want)
	}
}
