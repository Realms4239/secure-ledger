package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"secureledger/gateway/internal/audit"
	"secureledger/gateway/internal/authn"
	"secureledger/gateway/internal/dash"
	"secureledger/gateway/internal/fraud"
	"secureledger/gateway/internal/idempotency"
	"secureledger/gateway/internal/ipc"
	"secureledger/gateway/internal/policy"
	"secureledger/gateway/internal/ratelimit"
)

// EventTypeSagaStart is the only ring event type the gateway emits (proto/ipc-wire.md).
const (
	EventTypeSagaStart byte = 0x01
	EventTypeStepFail  byte = 0x03 // test-client injection only (spec §7 flow)
)

// maxAmount bounds transfers to what the engine can represent: the engine
// parses amounts as i64, so anything larger would 202-accept a transfer that
// can never apply. 999B minor units is far above any mobile-money ticket.
const maxAmount = 999_999_999_999

// maxBody caps JSON request bodies; the settlement CSV upload keeps its own
// 10MB cap. Without this a single giant body can OOM the handler.
const maxBody = 1 << 20
const maxUploadBody = 10 << 20

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
	fraud  *fraud.Store

	settlementDir string
	reportPath    string

	hub           *dash.Hub
	partitionFlag string

	limiter   *ratelimit.Limiter
	failInject bool

	ringFullTotal     atomic.Int64
	unauthorizedTotal atomic.Int64
	duplicateKeyTotal atomic.Int64
	fraudHoldTotal    atomic.Int64
	rateLimitedTotal  atomic.Int64
}

func NewTransfersHandler(secret []byte, idem *idempotency.Store, sink ipc.Sink, lg *audit.Logger, status StatusClient, fr *fraud.Store, settlementDir, reportPath string) *TransfersHandler {
	return &TransfersHandler{secret: secret, idem: idem, sink: sink, audit: lg, status: status, fraud: fr, settlementDir: settlementDir, reportPath: reportPath}
}

func (h *TransfersHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /transfers", h.limited(h.handleTransfer))
	// Fail injection is a test hook: any authenticated subject could fail any
	// saga, so the route only exists when explicitly enabled.
	if h.failInject {
		mux.HandleFunc("POST /transfers/{id}/fail", h.limited(h.handleTransferFail))
	}
	mux.HandleFunc("GET /sagas/{id}", h.limited(h.handleSagaStatus))
	mux.HandleFunc("POST /settlement/upload", h.limited(h.handleSettlementUpload))
	mux.HandleFunc("GET /report", h.limited(h.handleReport))
	mux.HandleFunc("POST /chaos/partition", h.limited(h.handleChaosPartition))
	mux.HandleFunc("GET /metricsz", h.handleMetricsz)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })
}

// SetRateLimiter attaches the per-IP limiter; nil disables (tests).
func (h *TransfersHandler) SetRateLimiter(l *ratelimit.Limiter) { h.limiter = l }

// SetFailInject enables the test-only fail-injection route. Must be called
// before Register; default off.
func (h *TransfersHandler) SetFailInject(on bool) { h.failInject = on }

func (h *TransfersHandler) limited(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.limiter != nil && !h.limiter.Allow(ratelimit.ClientIP(r.RemoteAddr)) {
			h.rateLimitedTotal.Add(1)
			h.audit.Log("rate_limited", "", r.URL.Path, "", "", 0)
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
			return
		}
		next(w, r)
	}
}

// SetHub attaches the dashboard event hub; nil (tests, minimal runs) disables
// event broadcast without touching intake behavior.
func (h *TransfersHandler) SetHub(hub *dash.Hub) { h.hub = hub }

// SetPartitionFlag sets the flag file the engine watches: present = network
// partition (engine pauses intake, catch-up replay on removal).
func (h *TransfersHandler) SetPartitionFlag(path string) { h.partitionFlag = path }

// emitTransfer publishes a lane event: everything the dashboard needs to
// draw one moving chip, nothing more. Nil-hub is a no-op by design.
func (h *TransfersHandler) emitTransfer(operator, txid string, amount float64, sagaID string) {
	if h.hub == nil {
		return
	}
	raw, _ := json.Marshal(map[string]any{
		"kind": "accepted", "operator": operator, "txid": txid,
		"amount": amount, "saga_id": sagaID,
	})
	h.hub.Publish(string(raw))
}

// emit publishes a dashboard event; nil-hub is a no-op by design.
func (h *TransfersHandler) emit(kind, subject, route, sagaID, detail string) {
	if h.hub == nil {
		return
	}
	raw, _ := json.Marshal(map[string]string{
		"kind": kind, "subject": subject, "route": route, "saga_id": sagaID, "detail": detail,
	})
	h.hub.Publish(string(raw))
}

type transferRequest struct {
	FromAccount string  `json:"from_account"`
	ToAccount   string  `json:"to_account"`
	Amount      float64 `json:"amount"`
	Operator    string  `json:"operator"`
	Txid        string  `json:"txid"`
}

type transferPayload struct {
	From           string  `json:"from"`
	To             string  `json:"to"`
	Amount         float64 `json:"amount"`
	IdempotencyKey string  `json:"idempotency_key"`
	Operator       string  `json:"operator"`
	Txid           string  `json:"txid"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// operatorOf defaults the originating operator for seed simplicity; real
// operator traffic always sets it explicitly.
func operatorOf(op string) string {
	if op == "" {
		return "mvola"
	}
	return op
}

// txidOf defaults the operator-side transaction id; the seed/loader sets it
// explicitly so settlement rows can join back to sagas.
func txidOf(txid string) string {
	if txid == "" {
		return uuid.NewString()
	}
	return txid
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil || req.FromAccount == "" || req.ToAccount == "" || req.Amount <= 0 {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "body must be {from_account,to_account,amount>0}")
		return
	}
	if req.Amount != math.Trunc(req.Amount) {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "amount must be integer minor units")
		return
	}
	if req.Amount > maxAmount {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "amount exceeds maximum")
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
		h.emit("duplicate", subject, "/transfers", existing, key)
		writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate_idempotency_key", "saga_id": existing})
		return
	}
	// Fraud plugin (Project A rules v1): scored after dedup so an identical
	// retry always answers 409 instead of re-scoring. A hold stays out of the
	// sink and the ledger entirely — audit + counter carry it to review.
	if held, reason := h.fraud.Check(subject, operatorOf(req.Operator), req.Amount, time.Now().Unix()); held {
		h.fraudHoldTotal.Add(1)
		h.audit.Log("fraud_hold", subject, "/transfers", "", key, time.Since(start).Milliseconds())
		h.emit("held_for_review", subject, "/transfers", "", reason)
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "held_for_review", "reason": reason})
		return
	}
	// ponytail: key is now mapped to sagaID before the ring write; on ErrRingFull a retry of
	// this key gets 409 pointing at a saga that never started. Store needs Delete to roll back — add if backpressure retries matter.

	// Resolve once: the payload and the lane event must carry the same ids.
	operator := operatorOf(req.Operator)
	txid := txidOf(req.Txid)
	payload, err := json.Marshal(transferPayload{
		From:           req.FromAccount,
		To:             req.ToAccount,
		Amount:         req.Amount,
		IdempotencyKey: key,
		Operator:       operator,
		Txid:           txid,
	})
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
	h.emitTransfer(operator, txid, req.Amount, sagaID.String())
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

// handleChaosPartition toggles the engine's partition flag (JWT-required:
// chaos is privileged). ON = engine pauses intake, mimicking a network
// partition; OFF = catch-up replay. Same file-drop rule as settlement.
func (h *TransfersHandler) handleChaosPartition(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	subject, err := authn.ValidateJWT(token, h.secret)
	if err != nil {
		h.unauthorizedTotal.Add(1)
		h.reject(w, start, "", http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token")
		return
	}
	if h.partitionFlag == "" {
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "partition flag unconfigured")
		return
	}
	var req struct {
		On bool `json:"on"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "body must be {on:bool}")
		return
	}
	if req.On {
		if err := os.WriteFile(h.partitionFlag, []byte("partition\n"), 0o644); err != nil {
			h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "flag write failed")
			return
		}
	} else if err := os.Remove(h.partitionFlag); err != nil && !os.IsNotExist(err) {
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "flag remove failed")
		return
	}
	h.audit.Log("partition", subject, "/chaos/partition", "", "", time.Since(start).Milliseconds())
	state := "off"
	if req.On {
		state = "on"
	}
	h.emit("partition", subject, "/chaos/partition", "", state)
	writeJSON(w, http.StatusAccepted, map[string]any{"partition": req.On})
}

func (h *TransfersHandler) handleMetricsz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "ring_full_total %d\nunauthorized_total %d\nduplicate_key_total %d\nfraud_hold_total %d\nrate_limited_total %d\n",
		h.ringFullTotal.Load(), h.unauthorizedTotal.Load(), h.duplicateKeyTotal.Load(), h.fraudHoldTotal.Load(), h.rateLimitedTotal.Load())
}

// handleSettlementUpload accepts a raw operator settlement CSV and drops it
// where the engine sweep picks it up. File-drop keeps the no-new-RPC rule:
// the engine never trusts the gateway beyond bytes on disk.
func (h *TransfersHandler) handleSettlementUpload(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	subject, err := authn.ValidateJWT(token, h.secret)
	if err != nil {
		h.unauthorizedTotal.Add(1)
		h.reject(w, start, "", http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxUploadBody))
	if err != nil || len(body) == 0 {
		h.reject(w, start, subject, http.StatusBadRequest, "bad_request", "body must be raw settlement CSV")
		return
	}
	if err := os.MkdirAll(h.settlementDir, 0o755); err != nil {
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "settlement dir unavailable")
		return
	}
	name := fmt.Sprintf("incoming-%d.csv", time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(h.settlementDir, name), body, 0o644); err != nil {
		h.reject(w, start, subject, http.StatusInternalServerError, "internal_error", "settlement write failed")
		return
	}
	h.audit.Log("settlement_uploaded", subject, "/settlement/upload", name, "", time.Since(start).Milliseconds())
	h.emit("settlement_uploaded", subject, "/settlement/upload", name, "")
	writeJSON(w, http.StatusAccepted, map[string]any{"file": name, "bytes": len(body)})
}

// handleReport serves the engine's latest match report verbatim. Absent file
// means no settlement has been processed yet — not an error in the ledger.
func (h *TransfersHandler) handleReport(w http.ResponseWriter, _ *http.Request) {
	raw, err := os.ReadFile(h.reportPath)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no settlement processed yet"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
