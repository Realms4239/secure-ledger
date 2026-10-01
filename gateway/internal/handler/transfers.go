package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"secureledger/gateway/internal/audit"
	"secureledger/gateway/internal/authn"
	"secureledger/gateway/internal/idempotency"
	"secureledger/gateway/internal/ipc"
	"secureledger/gateway/internal/policy"
)

// EventTypeSagaStart is the only ring event type the gateway emits (proto/ipc-wire.md).
const (
	EventTypeSagaStart byte = 0x01
	EventTypeStepFail  byte = 0x03 // test-client injection only (spec §7 flow)
)

// StatusClient proxies saga status lookups to the engine (real gRPC impl lands in Task 7).
type StatusClient interface {
	GetSagaStatus(ctx context.Context, sagaID string) (state string, steps []string, updatedAt string, err error)
}

type TransfersHandler struct {
	secret []byte
	idem   *idempotency.Store
	sink   ipc.Sink
	audit  *audit.Logger
	status StatusClient

	ringFullTotal     atomic.Int64
	unauthorizedTotal atomic.Int64
	duplicateKeyTotal atomic.Int64
}

func NewTransfersHandler(secret []byte, idem *idempotency.Store, sink ipc.Sink, lg *audit.Logger, status StatusClient) *TransfersHandler {
	return &TransfersHandler{secret: secret, idem: idem, sink: sink, audit: lg, status: status}
}

func (h *TransfersHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /transfers", h.handleTransfer)
	mux.HandleFunc("POST /transfers/{id}/fail", h.handleTransferFail)
	mux.HandleFunc("GET /sagas/{id}", h.handleSagaStatus)
	mux.HandleFunc("GET /metricsz", h.handleMetricsz)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })
}

type transferRequest struct {
	FromAccount string  `json:"from_account"`
	ToAccount   string  `json:"to_account"`
	Amount      float64 `json:"amount"`
}

type transferPayload struct {
	From           string  `json:"from"`
	To             string  `json:"to"`
	Amount         float64 `json:"amount"`
	IdempotencyKey string  `json:"idempotency_key"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *TransfersHandler) reject(w http.ResponseWriter, start time.Time, subject string, code int, decision, msg string) {
	h.audit.Log(decision, subject, "/transfers", "", "", time.Since(start).Milliseconds())
	writeJSON(w, code, map[string]string{"error": msg})
}

func (h *TransfersHandler) handleTransfer(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	subject, err := authn.ValidateJWT(token, h.secret)
	if err != nil {
		h.unauthorizedTotal.Add(1)
		h.reject(w, start, "", http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token")
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if _, err := uuid.Parse(key); err != nil {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "Idempotency-Key must be a UUID")
		return
	}

	var req transferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.FromAccount == "" || req.ToAccount == "" || req.Amount <= 0 {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "body must be {from_account,to_account,amount>0}")
		return
	}
	if req.Amount != math.Trunc(req.Amount) {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "amount must be integer minor units")
		return
	}

	if !policy.Authorized(subject, req.FromAccount) {
		h.reject(w, start, subject, http.StatusForbidden, "forbidden", "subject may not debit from_account")
		return
	}

	sagaID := uuid.New()
	dup, existing := h.idem.CheckOrStore(key, sagaID.String())
	if dup {
		h.duplicateKeyTotal.Add(1)
		h.audit.Log("duplicate", subject, "/transfers", existing, key, time.Since(start).Milliseconds())
		writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate_idempotency_key", "saga_id": existing})
		return
	}
	// ponytail: key is now mapped to sagaID before the ring write; on ErrRingFull a retry of
	// this key gets 409 pointing at a saga that never started. Store needs Delete to roll back — add if backpressure retries matter.

	payload, err := json.Marshal(transferPayload{req.FromAccount, req.ToAccount, req.Amount, key})
	if err != nil {
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "payload marshal failed")
		return
	}
	if err := h.sink.Write(EventTypeSagaStart, [16]byte(sagaID), payload); err != nil {
		if errors.Is(err, ipc.ErrSinkFull) {
			h.ringFullTotal.Add(1)
			h.reject(w, start, subject, http.StatusServiceUnavailable, "ring_full", "backpressure: engine ring full")
			return
		}
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "ring write failed")
		return
	}

	h.audit.Log("accepted", subject, "/transfers", sagaID.String(), key, time.Since(start).Milliseconds())
	writeJSON(w, http.StatusAccepted, map[string]string{"saga_id": sagaID.String(), "status": "accepted"})
}

// handleTransferFail injects STEP_FAIL into the ring for an existing saga
// (test-client hook per spec §7; a real downstream failure produces the same event).
func (h *TransfersHandler) handleTransferFail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	subject, err := authn.ValidateJWT(token, h.secret)
	if err != nil {
		h.unauthorizedTotal.Add(1)
		h.reject(w, start, "", http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token")
		return
	}
	sagaID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "saga id must be a UUID")
		return
	}
	if err := h.sink.Write(EventTypeStepFail, [16]byte(sagaID), nil); err != nil {
		if errors.Is(err, ipc.ErrSinkFull) {
			h.ringFullTotal.Add(1)
			h.reject(w, start, subject, http.StatusServiceUnavailable, "ring_full", "backpressure: engine ring full")
			return
		}
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "ring write failed")
		return
	}
	h.audit.Log("fail_injected", subject, "/transfers/"+sagaID.String()+"/fail", sagaID.String(), "", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusAccepted, map[string]string{"saga_id": sagaID.String(), "status": "fail_injected"})
}

func (h *TransfersHandler) handleSagaStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, steps, updatedAt, err := h.status.GetSagaStatus(r.Context(), id)
	if err != nil {
		// ponytail: every client error maps to 404 until Task 7 distinguishes NotFound from transport errors
		h.audit.Log("not_found", "", r.URL.Path, id, "", 0)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "saga not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"saga_id": id, "state": state, "steps": steps, "updated_at": updatedAt})
}

func (h *TransfersHandler) handleMetricsz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "ring_full_total %d\nunauthorized_total %d\nduplicate_key_total %d\n",
		h.ringFullTotal.Load(), h.unauthorizedTotal.Load(), h.duplicateKeyTotal.Load())
}
