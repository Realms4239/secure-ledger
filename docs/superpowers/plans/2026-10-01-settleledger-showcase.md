# SettleLedger Showcase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Windows-demoable settlement showcase from spec `docs/superpowers/specs/2026-10-01-settleledger-showcase-design.md`: file transport, CSV matcher, Postgres truth, fraud plugin, live dashboard, recorded stress, OSS polish.

**Architecture:** Gateway gains an `ipc.Sink` interface (ring = WSL2 bench path, file = Windows demo path) selected by `TRANSPORT` env; engine gains a file tailer with persisted offset plus `settle` (CSV matcher) and `pg` (Postgres sink) modules; dashboard is gateway static HTML + SSE over existing state; stress is a stdlib Go loader plus committed bench logs.

**Tech Stack:** Go 1.26.5 stdlib + existing deps (jwt, uuid, grpc client — no new Go deps); Rust stable (existing tonic/tokio/serde + new `tokio-postgres 0.7` only); Postgres 16 via compose; PowerShell 5.1 scripts for Windows gates.

## Global Constraints

- Local cargo invocations MUST set `CARGO_TARGET_DIR=D:/sl-target/<worktree-name>` — C: is critically low on disk.
- `TRANSPORT=file|shm` env on gateway and engine, default `file` on Windows, `shm` only on WSL2/Linux.
- `proto/ipc-wire.md` and shm ring code are FROZEN — no edits to `gateway/internal/ipc/ring.go`, `wire.go`, `engine/src/ring.rs`, `engine/src/wire.rs`.
- JWT HS256 + single policy stay; no OIDC/mTLS/JWKS in this plan.
- No metric text ("p99", ">99.9%") anywhere except inside `bench/results/` reports and dashboard "last measured" panel with date/hardware.
- No `TODO`/`TBD`; conventional commits (`feat:`/`fix:`/`docs:`/`test:`/`chore:`); work only inside the worktree, merge to main only in Phase 1 Task 1.
- Verification per task: `go vet` + `go test` for gateway; `cargo clippy -- -D warnings` + `cargo test` for engine.

---

## File Structure

**Phase 1 — land tracer (merge + CI + baseline):**
- Modify: none (commit leftovers as-is, then merge `agent/tracer-bullet` → `main`)
- Create: `.github/workflows/ci.yml`, `bench/results/baseline/<logs>`, fresh `.worktrees/settleledger` on `agent/settleledger`

**Phase 2 — showcase:**
- `gateway/internal/ipc/sink.go` — `Sink` iface + `ErrSinkFull` alias (new)
- `gateway/internal/ipc/filesink.go` — `FileSink` JSONL appender (new)
- Modify: `gateway/internal/handler/transfers.go` (`ring *RingWriter` → `sink Sink`), `transfers_test.go` setup, `gateway/cmd/gateway/main.go` (TRANSPORT select)
- `gateway/internal/fraud/rules.go` — velocity/threshold/geo checks + window store (new)
- `gateway/internal/dash/dash.go` + `gateway/web/dashboard.html` + `gateway/web/bench-latest.json` — SSE + page (new)
- `gateway/cmd/seed/main.go` — deterministic CSV + expected-report generator (new)
- `gateway/cmd/loader/main.go` — stdlib stress loader (new, Phase 3 use; built here)
- `engine/src/tail.rs` — file tailer with offset (new)
- `engine/src/settle.rs` — CSV ingest + matcher (new)
- `engine/src/pg.rs` — Postgres sink with degraded mode (new, dep: `tokio-postgres 0.7`)
- Modify: `engine/src/main.rs` (TRANSPORT select, module wiring), `engine/src/lib.rs` (new mods), `engine/Cargo.toml` (one dep)
- `sample-data/` — golden CSV + expected report (generated, committed)
- `compose.yaml` — postgres:16-alpine (new)
- `scripts/showcase.ps1` — Windows end-to-end gate (new)

**Phase 3 — proof:**
- `bench/results/<date>/` — loader output + summary (committed logs)
- `scripts/recovery.ps1` — kill-9 drill asserting identical states (new)
- `LICENSE` (MIT), `SECURITY.md`, `CONTRIBUTING.md`, README demo section, `docs/architecture.md`, `docs/integration.md`, `docs/limitations.md` extension (new/modify)

---

### Task 1: Commit tracer leftovers, merge to main, fresh worktree, Windows CI

**Files:**
- Commit as-is on `agent/tracer-bullet`: `gateway/cmd/gateway/main.go`, `gateway/go.mod`, `gateway/go.sum`, `gateway/internal/ipc/ring.go`, `gateway/cmd/ringpeek/`, `scripts/`
- Create: `.github/workflows/ci.yml`
- New worktree: `.worktrees/settleledger` on branch `agent/settleledger` from `main`

**Interfaces:**
- Consumes: verified-green tree (go vet/test, cargo clippy/test pass with leftovers in place)
- Produces: `main` containing tracer; CI green badge path; clean lane for Phase 2

- [ ] **Step 1: Re-verify the dirty tree before committing it**

Run (gateway): `go vet ./...` then `go test ./internal/policy/ ./internal/idempotency/ ./internal/audit/ ./internal/authn/ ./internal/ipc/ ./internal/handler/` — Expected: all ok.
Run (engine, from worktree root): `CARGO_TARGET_DIR=D:/sl-target/tracer-engine cargo clippy -- -D warnings` then `CARGO_TARGET_DIR=D:/sl-target/tracer-engine cargo test` — Expected: clean, 20 pass. (If anything fails, stop — do not commit red code.)

- [ ] **Step 2: Commit leftovers as one honest commit**

```bash
git add gateway/cmd/gateway/main.go gateway/go.mod gateway/go.sum gateway/internal/ipc/ring.go gateway/cmd/ringpeek/ scripts/
git commit -m "feat: tracer wiring leftovers (grpc status client, fail route, ringpeek, ps1 harness)"
```

- [ ] **Step 3: Write `.github/workflows/ci.yml`**

```yaml
name: ci
on: [push, pull_request]
jobs:
  gateway:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.26.5' }
      - run: go vet ./...
        working-directory: gateway
      - run: go test ./...
        working-directory: gateway
  engine:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: arduino/setup-protoc@v3
      - uses: dtolnay/rust-toolchain@stable
        with: { components: clippy }
      - run: cargo clippy -- -D warnings
        working-directory: engine
      - run: cargo test
        working-directory: engine
```

- [ ] **Step 4: Commit CI, merge to main (in C:\DRIFT, still on main)**

```bash
git add .github/workflows/ci.yml && git commit -m "chore: windows CI for gateway and engine"
git merge agent/tracer-bullet --no-ff -m "feat: tracer bullet (jwt gateway, shm ring, rust saga/wal/grpc)"
git worktree remove .worktrees/tracer-bullet && git branch -d agent/tracer-bullet
git worktree add .worktrees/settleledger -b agent/settleledger
```

Expected: `git worktree list` shows `main` + `settleledger`; `git log --oneline -3` on main shows the merge. All further tasks run in `C:\DRIFT\.worktrees\settleledger` with `CARGO_TARGET_DIR=D:/sl-target/settleledger`.

- [ ] **Step 5: Baseline logs**

Run the full suites once more in the new worktree, saving output:
`go test ./... > bench/results/baseline/go-test.log 2>&1` and `cargo test > bench/results/baseline/cargo-test.log 2>&1` (with target-dir env). Commit: `git add bench/results/baseline .github && git commit -m "chore: baseline test logs post-merge"` (if CI file merged already, just baseline).

**Oracle gate 1:** tracer sound on Windows CI? Evidence: CI run link or local logs in `bench/results/baseline/`.

---

### Task 2: Gateway `ipc.Sink` abstraction + `FileSink`

**Files:**
- Create: `gateway/internal/ipc/sink.go`, `gateway/internal/ipc/filesink.go`, `gateway/internal/ipc/filesink_test.go`
- Modify: `gateway/internal/handler/transfers.go` (field type + 503 check), `gateway/internal/handler/transfers_test.go` (setup), `gateway/cmd/gateway/main.go` (TRANSPORT select)

**Interfaces:**
- Consumes: `RingWriter.Write(typ byte, sagaID [16]byte, payload []byte) error`, `ipc.ErrRingFull`
- Produces: `type Sink interface { Write(typ byte, sagaID [16]byte, payload []byte) error }`; `var ErrSinkFull = ErrRingFull`; `func NewFileSink(path string) (*FileSink, error)` (FileSink implements Sink; disk errors returned raw, never ErrSinkFull)

- [ ] **Step 1: Write `gateway/internal/ipc/sink.go`**

```go
package ipc

// Sink is the event transport. RingWriter (shm bench path) and FileSink
// (Windows demo path) both implement it. Handler code uses Sink only.
type Sink interface {
	Write(typ byte, sagaID [16]byte, payload []byte) error
}

// ErrSinkFull reports honest backpressure; identical to ErrRingFull so
// existing 503 mapping keeps working for both transports.
var ErrSinkFull = ErrRingFull
```

- [ ] **Step 2: Write failing test `gateway/internal/ipc/filesink_test.go`**

```go
package ipc

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestFileSink_AppendsJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	s, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := uuid.New()
	payload := []byte(`{"from":"alice","to":"bob","amount":100,"idempotency_key":"k"}`)
	if err := s.Write(0x01, [16]byte(id), payload); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		lines++
		var v map[string]any
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatalf("line is not JSON: %v", err)
		}
		if v["id"] != id.String() || v["t"] != float64(1) {
			t.Fatalf("bad line: %s", sc.Text())
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(v["p"].(string)), &p); err != nil {
			t.Fatalf("payload not embedded JSON: %v", err)
		}
	}
	if lines != 1 {
		t.Fatalf("want 1 line got %d", lines)
	}
}
```

- [ ] **Step 3: Run to verify fail** — `go test ./internal/ipc/ -run TestFileSink -v` — Expected: FAIL `undefined: NewFileSink`.

- [ ] **Step 4: Implement `gateway/internal/ipc/filesink.go`**

```go
package ipc

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// FileSink appends one JSON object per event: {"t":1,"id":"<uuid>","ts":<ns>,"p":"{...}"}.
// Same schema the engine tailer parses; payload bytes are embedded verbatim as a string.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
}

func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

func (s *FileSink) Write(typ byte, sagaID [16]byte, payload []byte) error {
	v := map[string]any{
		"t":  typ,
		"id": uuid.UUID(sagaID).String(),
		"ts": time.Now().UnixNano(),
		"p":  string(payload),
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.f.Write(append(raw, '\n'))
	return err
}

func (s *FileSink) Close() error { return s.f.Close() }
```

- [ ] **Step 5: Rewire handler to `Sink`** — in `transfers.go`: field `ring *ipc.RingWriter` → `sink ipc.Sink`; constructor param likewise; two call sites `h.ring.Write` → `h.sink.Write`; `errors.Is(err, ipc.ErrRingFull)` → `errors.Is(err, ipc.ErrSinkFull)`. In `transfers_test.go`: `deps` field `ring *ipc.RingWriter` stays for ring-specific tests; `setup` passes `ring` (it implements Sink — no other change needed since `*RingWriter` satisfies the interface). Verify compile proves it.

- [ ] **Step 6: `main.go` TRANSPORT select** — replace ring construction with:

```go
transport := os.Getenv("TRANSPORT")
if transport == "" {
	transport = "shm"
}
var sink ipc.Sink
var ringPath string
switch transport {
case "file":
	eventsPath := os.Getenv("EVENTS_PATH")
	if eventsPath == "" {
		eventsPath = "data/events.log"
	}
	fs, err := ipc.NewFileSink(eventsPath)
	if err != nil {
		log.Fatalf("open events file %s: %v", eventsPath, err)
	}
	defer fs.Close()
	sink = fs
	log.Printf("gateway transport=file events=%s", eventsPath)
default:
	ringPath := os.Getenv("RING_PATH")
	if ringPath == "" {
		if runtime.GOOS == "windows" {
			ringPath = filepath.Join(os.TempDir(), "secureledger.ring")
		} else {
			ringPath = "/dev/shm/secureledger.ring"
		}
	}
	ring, err := ipc.NewRingWriter(ringPath, defaultRingSlots)
	if err != nil {
		log.Fatalf("open ring %s: %v", ringPath, err)
	}
	defer ring.Close()
	sink = ring
	log.Printf("gateway transport=shm ring=%s", ringPath)
}
```

then `handler.NewTransfersHandler([]byte(secret), idempotency.New(), sink, audit.New(os.Stdout), grpcStatus)`. Keep the existing listen log line but drop `ring=%s` (variable no longer exists).

- [ ] **Step 7: Run** `go vet ./... && go test ./...` — Expected: PASS (old ring tests prove Sink-compat).

- [ ] **Step 8: Commit** `git add gateway/internal/ipc/ gateway/internal/handler/ gateway/cmd/gateway/ && git commit -m "feat: file transport sink for windows demo (shm untouched)"`

---

### Task 3: Engine file tailer + TRANSPORT select

**Files:**
- Create: `engine/src/tail.rs` (with inline `#[cfg(test)]`)
- Modify: `engine/src/main.rs`, `engine/src/lib.rs`, engine consumer dispatch

**Interfaces:**
- Consumes: `SagaStore.apply(typ: u8, saga_id: &[u8;16], payload: &[u8])`, file lines from Task 2 format
- Produces: `pub struct FileTailer` with `pub fn open(events: PathBuf, offset: PathBuf) -> Result<Self, TailError>` and `pub fn next(&mut self) -> Result<Option<TailEvent>, TailError>`; `pub struct TailEvent { pub typ: u8, pub saga_id: [u8; 16], pub payload: Vec<u8> }`; `TRANSPORT=file` (default on Windows) vs `shm` in main

- [ ] **Step 1: Write failing test in `engine/src/tail.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn tail_reads_one_event_then_none_and_resumes_from_offset() {
        let dir = std::env::temp_dir().join(format!("tailtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ev = dir.join("events.log");
        let off = dir.join("events.offset");
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&ev).unwrap();
        writeln!(f, r#"{{"t":1,"id":"550e8400-e29b-41d4-a716-446655440000","ts":1700000000000000000,"p":"{{\\"from\\":\\"alice\\"}}"}}"#).unwrap();
        drop(f);
        let mut tail = FileTailer::open(ev.clone(), off.clone()).unwrap();
        let e = tail.next().unwrap().expect("one event");
        assert_eq!(e.typ, 0x01);
        assert_eq!(hex::encode(e.saga_id), "550e8400e29b41d4a716446655440000");
        assert!(tail.next().unwrap().is_none());
        drop(tail);
        // reopen: offset persisted, still none (no re-delivery)
        let mut tail2 = FileTailer::open(ev, off).unwrap();
        assert!(tail2.next().unwrap().is_none());
    }
}
```

- [ ] **Step 2: Run fail** — `cargo test tail` (with target-dir env) — Expected: FAIL `can't find crate for tail` / unresolved module (wire `pub mod tail;` in lib.rs + main.rs first so it compiles-to-fail on missing items, or add mod then fail on missing FileTailer).

- [ ] **Step 3: Implement `engine/src/tail.rs`** — parse each line as JSON `{t:number, id:uuid-string, ts:number, p:string}`; uuid-string → `[u8;16]` via exact helper (no new dep):

```rust
fn parse_uuid_hex(s: &str) -> Result<[u8; 16], TailError> {
    let h: String = s.chars().filter(|c| *c != '-').collect();
    if h.len() != 32 {
        return Err(TailError::BadId(s.to_string()));
    }
    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&h[2 * i..2 * i + 2], 16)
            .map_err(|_| TailError::BadId(s.to_string()))?;
    }
    Ok(id)
}
```

Tailer holds file offset in memory, persists every consumed offset to the offset file (write whole `u64` LE), opens events with read+seek to stored offset, polls (return None when at EOF; caller sleeps like the ring consumer). Bad line → skip + count (return `Err(TailError::CorruptLine(n))` and advance past it — caller logs and continues, mirroring CorruptSlot handling).

- [ ] **Step 4: Wire `main.rs`** — read `TRANSPORT` (default: `file` if Windows else `shm`; simplest: default `file` always — shm only when explicitly set, since shm needs pre-created ring):

```rust
let transport = env_or("TRANSPORT", "file");
```

Spawn consumer: `if transport == "file" { run_file_consumer(events_path, store) } else { run_ring_consumer(ring_path, store) }` where `run_file_consumer` mirrors `run_ring_consumer` but calls `tail.next()` and `guard.apply(e.typ, &e.saga_id, &e.payload)`. `EVENTS_PATH` default `data/events.log`, `EVENTS_OFFSET` default `data/events.offset`.

- [ ] **Step 5: Run** `cargo clippy -- -D warnings && cargo test` — Expected: PASS (ring tests untouched).

- [ ] **Step 6: Commit** `git add engine/src/tail.rs engine/src/main.rs engine/src/lib.rs && git commit -m "feat: file transport tailer with persisted offset"`

---

### Task 4: Settlement CSV ingest + matcher (`engine/src/settle.rs`)

**Files:**
- Create: `engine/src/settle.rs` (with tests), extend `engine/src/grpc.rs` or new `POST /settlement/upload` on gateway (gateway side in Task 6)
- Modify: `engine/src/lib.rs`, `engine/src/main.rs` (hold matched state + expose via status `steps`)

**Interfaces:**
- Consumes: `SagaStore.get(&id) -> Option<Saga>` (payload has from/to/amount — engine must retain amounts: extend `Saga` with `amount: i64` + `to: String` parsed at apply time; WAL entry gains `amount`/`to` fields — replay fills them, old WAL lines default 0/"")
- Produces: `pub struct SettlementRow { pub operator: String, pub txid: String, pub from: String, pub to: String, pub amount: i64, pub fee: i64, pub ts: u64 }`; `pub enum Verdict { Settled, SettledTolerance(String), Discrepancy(DisClass) }`; `pub enum DisClass { Missing, Orphan, Mismatch }`; `pub fn parse_csv(text: &str) -> (Vec<SettlementRow>, u64 corrupt)`; `pub fn match_row(row: &SettlementRow, sagas: &HashMap<String, SagaView>) -> Verdict`; counters struct `MatchReport { settled, tolerated, missing, orphan, mismatch }`

- [ ] **Step 1: Failing matcher test**

```rust
#[test]
fn exact_tolerance_and_classes() {
    // saga "s1": alice->bob 10000 (cents), saga "s2": carol->dave 5000
    // row exact (txid s1, amount 10000) -> Settled
    // row fee-shifted (txid s2, amount 4950, fee 50, tol 100) -> SettledTolerance
    // row unknown txid -> Discrepancy(Orphan); saga s1 with no row -> Missing (via sweep fn)
    // row txid s1 amount 9999 -> Discrepancy(Mismatch)
}
```

- [ ] **Step 2: Run fail** — Expected: missing module/items.

- [ ] **Step 3: Implement** — hand-rolled CSV split (generator-owned invariant: no quoted commas; `parse_csv` rejects lines whose field count != 7 and counts corrupt). Matching key: gateway must embed operator txid — extend transfer payload with `operator` + `txid` fields (gateway `transferPayload` gains `operator string, txid string`; engine `Payload` gains `#[serde(default)] operator/txid`; `Saga` gains `txid: String, amount: i64`; WAL entry gains them too). Tolerance rule: `abs(row.amount + row.fee - saga.amount) <= tol_cents (default 100)` and `abs(row.ts - saga.updated_at_ns) <= skew_ns (default 5*60*1e9)` → SettledTolerance("fee"/"skew"). `sweep_missing(sagas, matched_ids) -> Vec<Discrepancy::Missing>`.

- [ ] **Step 4: Expose in status** — engine keeps `MatchReport` in Arc<Mutex<...>> alongside store; new gRPC `GetMatchReport` RPC? That changes `ledger.proto` (allowed — additive, version-minor; shm wire stays frozen). Simpler ponytail: NO proto change — gateway `GET /report` proxies an engine HTTP debug endpoint? Engine has no HTTP. Decision (locked here): extend `GetSagaStatusResponse.steps` already carries match notes per saga (`"matched:exact txid"`, `"discrepancy:mismatch"` pushed at match time), and add gateway-side `GET /report` that aggregates per-saga states by polling engine for known IDs (gateway tracks accepted saga IDs in-mem for the demo). No proto change. Document this trade-off in code comment.

- [ ] **Step 5: Run** clippy + test — Expected: PASS.

- [ ] **Step 6: Commit** `git add engine/src/settle.rs engine/src/saga.rs engine/src/lib.rs engine/src/main.rs gateway/internal/handler/ && git commit -m "feat: settlement csv matcher with tolerance verdicts"`

---

### Task 5: Postgres sink + compose (queryable truth)

**Files:**
- Create: `engine/src/pg.rs`, `compose.yaml`
- Modify: `engine/Cargo.toml` (add `tokio-postgres = "0.7"`), `engine/src/main.rs`, `engine/src/lib.rs`

**Interfaces:**
- Consumes: `Saga` + `MatchReport` snapshots
- Produces: `pub struct PgSink` with `pub async fn connect(url: &str) -> Result<Self, PgError>`, `pub async fn ensure_schema(&self)`, `pub async fn upsert_saga(&self, saga: &Saga, verdict: &str)`; main loop task flushing every 2s; `pg_degraded: AtomicBool` when unreachable (engine keeps serving from WAL; backfills by full re-flush on reconnect)

- [ ] **Step 1: `compose.yaml`**

```yaml
services:
  pg:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ledger
      POSTGRES_PASSWORD: ledger
      POSTGRES_DB: ledger
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]
volumes:
  pgdata:
```

- [ ] **Step 2: Schema + failing connection test** — `ensure_schema` creates `sagas(saga_id PK, state, amount, txid, verdict, updated_at_ns)`; test connects to `DATABASE_URL` (skipped with notice when unset: `if url.is_empty() { eprintln!("pg test skipped"); return; }` — integration truth comes from showcase.ps1).

- [ ] **Step 3: Implement `pg.rs`** with `tokio-postgres = "0.7"`, `NoTls` (local demo; `docs/limitations.md` notes TLS for any real deployment).

- [ ] **Step 4: Wire flush task in main** — `DATABASE_URL` empty → skip PG entirely (log line), else spawn 2s flusher; degraded counter `pg_degraded_total` printed in replay-style log line.

- [ ] **Step 5: Run** clippy + test (PG test skips without DB) — Expected: PASS.

- [ ] **Step 6: Commit** `git add engine/src/pg.rs engine/Cargo.toml engine/Cargo.lock engine/src/main.rs engine/src/lib.rs compose.yaml && git commit -m "feat: postgres sink with degraded mode"`

---

### Task 6: Fraud rules plugin + settlement-upload + report routes (gateway)

**Files:**
- Create: `gateway/internal/fraud/rules.go`, `gateway/internal/fraud/rules_test.go`
- Modify: `gateway/internal/handler/transfers.go` (fraud check + 2 routes), `transfers_test.go`

**Interfaces:**
- Consumes: `transferRequest`, `ipc.Sink`
- Produces: `func VelocityExceeded(hist []int64, now int64) bool` (>5 events in trailing 60s); `func AmountExceeds(amount float64, ageDays int) bool` (table: <30d → 50000, <365d → 500000, else no cap; amounts in MGA minor units); `func GeoImpossible(prevOp string, prevTs, curOp string, curTs int64) bool` (prevOp != curOp && curTs-prevTs < 30s); `type Store` (mutex map sender → {times, lastOp, lastTs, ageDays}) with `Check(sender, op string, amount float64, now int64) (held bool, reason string)`; held → `202 {"status":"held_for_review"}` + audit `fraud_hold` + `fraud_hold_total`, NO sink write

- [ ] **Step 1: Failing rules tests** — table: 6 rapid events → true; 5 → false; new account 60000 → true; geo hop 10s → true, 60s → false.

- [ ] **Step 2–3: Implement, run** `go test ./internal/fraud/ -v` — Expected: PASS.

- [ ] **Step 4: Handler wiring** — after policy check, before idempotency: read `X-Operator` header (default `"mvola"`), call `fraud.Check`; held → audit + 202 held. Add `POST /settlement/upload` (JWT-required; body = raw CSV; forwards bytes to engine — engine needs an ingest path: engine gRPC `IngestSettlement`? NO proto change per Task 4 decision — instead gateway writes CSV to `SETTLEMENT_DIR/incoming-<ts>.csv` and engine watches that dir every 5s (simplest file-drop, no new RPC). Document trade-off.) Add `GET /report` aggregating in-mem accepted IDs via StatusClient + counters.

- [ ] **Step 5: Run** full `go vet + go test` — Expected: PASS.

- [ ] **Step 6: Commit** `git add gateway/internal/fraud/ gateway/internal/handler/ && git commit -m "feat: fraud rules plugin and settlement routes"`

**Oracle gate 2 (after Tasks 2–6 + 7):** matching correct, demo reproducible in 5 min? Evidence: showcase.ps1 green run.

---

### Task 7: Dashboard (static HTML + SSE)

**Files:**
- Create: `gateway/web/dashboard.html`, `gateway/web/bench-latest.json`, `gateway/internal/dash/dash.go`, `gateway/internal/dash/dash_test.go`
- Modify: `gateway/cmd/gateway/main.go` (register dash)

**Interfaces:**
- Consumes: in-mem event hub (gateway intake/decision events), `/metricsz` counters, `GET /report` JSON
- Produces: `GET /dashboard` (embedded HTML via `//go:embed`), `GET /events/stream` (SSE, 15s client-side poll fallback note in HTML comment); `bench-latest.json` shape `{"measured":false}` until Phase 3 writes `{"measured":true,"date":"","hardware":"","p50_ms":0,"p95_ms":0,"p99_ms":0,"rps":0,"match_rate":0}`; dashboard renders "not yet measured" when false

- [ ] **Step 1: Failing test** — `GET /events/stream` returns 200 + `Content-Type: text/event-stream` and replays one hub event; `GET /dashboard` returns 200 + contains `<title>SettleLedger</title>`.

- [ ] **Step 2: Implement hub** (buffered channel + mutex subscriber list, cap 16, drop-slowest — `ponytail:` comment) + handlers; HTML page (single file, no JS deps: EventSource + fetch for report/metricsz, chaos buttons POST partition toggle → engine? partition toggle needs engine control: engine watches `data/partition.flag` file presence (created/deleted by dashboard POST handler) — file-drop again, consistent with Task 6. Kill-9 stays a documented manual drill (Task 10 script does it).

- [ ] **Step 3: Run** `go vet + go test` — Expected: PASS. Manual: `TRANSPORT=file go run ./cmd/gateway` + open `/dashboard`.

- [ ] **Step 4: Commit** `git add gateway/web/ gateway/internal/dash/ gateway/cmd/gateway/ && git commit -m "feat: live dashboard with sse and chaos controls"`

---

### Task 8: Seed generator + golden CSV + `showcase.ps1` gate

**Files:**
- Create: `gateway/cmd/seed/main.go`, `sample-data/settlement-10k.csv`, `sample-data/expected-report.json`, `scripts/showcase.ps1`
- Modify: none

**Interfaces:**
- Consumes: matcher rules (Task 4), loader path (Task 9 builds loader; showcase uses `go run ./cmd/loader` until binary committed — order note: implement Task 9 loader BEFORE this gate runs, or seed posts directly; decision: showcase.ps1 calls `go run ./cmd/loader` so no binary dependency)
- Produces: deterministic 10k-transfer seed (fixed RNG 42): 9,850 clean, 100 duplicate-key retries (same key twice), 25 amount-mismatch, 15 orphan rows, 10 corrupt lines; `expected-report.json` byte-compared after run

- [ ] **Step 1: Implement `cmd/seed`** — flags `-n -out -report`; writes CSV `operator,txid,from,to,amount,fee,ts` + expected JSON `{settled, tolerated, missing, orphan, mismatch, corrupt}` computed by CALLING the same rules (duplicate the 6-line tolerance predicate in Go with a comment pointing at `settle.rs` — cross-language duplication is intentional and byte-tested by the gate).

- [ ] **Step 2: Generate + commit sample data** — `go run ./cmd/seed -n 10000`, verify files exist, commit.

- [ ] **Step 3: Write `scripts/showcase.ps1`** — boots gateway(file)+engine+pg(compose), runs loader seed, uploads CSV (copy to SETTLEMENT_DIR), polls `/report` until counts settle (timeout 120s), compares with expected JSON (exact match or FAIL), prints PASS. Windows-native (PowerShell 5.1, no WSL).

- [ ] **Step 4: Run gate** — Expected: PASS. (First run WILL surface integration bugs — fix forward in task scope, then re-run.)

- [ ] **Step 5: Commit** `git add gateway/cmd/seed/ sample-data/ scripts/showcase.ps1 && git commit -m "feat: golden seed data and windows showcase gate"`

---

### Task 9: Stress loader + recorded bench results

**Files:**
- Create: `gateway/cmd/loader/main.go`, `bench/results/<date>/loader.json`, `bench/results/<date>/summary.md`
- Modify: `gateway/web/bench-latest.json` (measured:true + numbers)

**Interfaces:**
- Consumes: `POST /transfers` (file transport), `GET /report`
- Produces: loader flags `-url -jwt-secret -n -c -operator`; stdout + `loader.json` `{n, concurrency, p50_ms, p95_ms, p99_ms, rps, errors}`; engine replay timer from engine log line `replay 1M events in Xs` (measure: feed 1M seeded file events, time tail consumption via `replay_duration_ms` counter — add counter in Task 3 tailer? NOTE: add `consumed_total` + start-time log in tail consumer during this task, small engine edit allowed)

- [ ] **Step 1: Implement loader** (stdlib only: goroutines, `net/http`, HMAC JWT minted in-process with `golang-jwt` — already a dep):

```go
// sketch (full code in task): TigerBeetle-style closed loop per worker,
// latencies slice guarded by mutex, sort.Float64s, percentiles.
```

(full implementation ~120 lines — write it completely in the task; no sketch in final code.)

- [ ] **Step 2: Run** `go run ./cmd/loader -n 10000 -c 50` against local file-transport gateway — Expected: completes, prints p50/p95/p99 + rps.

- [ ] **Step 3: Record** — 10-min sustained run (or documented shorter first run: `-n 100000`), save `loader.json` + `summary.md` (hardware: `powershell Get-ComputerInfo` one-liner output pasted), update `bench-latest.json`, commit `git add bench/results gateway/web/bench-latest.json && git commit -m "test: recorded stress <date> (p99=… rps=… match=…)"`.

---

### Task 10: Recovery drill script + log

**Files:**
- Create: `scripts/recovery.ps1`
- Modify: `bench/results/<date>/` (append drill log)

**Interfaces:**
- Consumes: running showcase stack, `GET /report`
- Produces: script that snapshots `/report`, `Stop-Process` engine (kill -9 analogue), restarts engine, replays, compares reports byte-identical → PASS/FAIL; committed `recovery.log`

- [ ] **Step 1: Write script** (~40 lines PS: Invoke-RestMethod snapshots, kill, restart with same env, poll, Compare-Object).
- [ ] **Step 2: Run** — Expected: PASS, states identical (proves RPO = last record).
- [ ] **Step 3: Commit** log + script.

---

### Task 11: OSS polish + README + docs

**Files:**
- Create: `LICENSE` (MIT — decision flagged: change now if you want GPL/Apache), `SECURITY.md`, `CONTRIBUTING.md`, `docs/architecture.md`, `docs/integration.md`
- Modify: `README.md` (5-min demo), `docs/limitations.md` (file-transport ceiling from bench, PG-TLS note, 10-min sustained scope)

- [ ] **Step 1: Write files** (README demo block: `git clone …; docker compose up -d; TRANSPORT=file …; open /dashboard`).
- [ ] **Step 2: `grep -r "TODO\|TBD"`** — Expected: empty (except this plan's history).
- [ ] **Step 3: Full verification** — `go vet + go test`, `cargo clippy + cargo test`, `showcase.ps1`, `recovery.ps1` — all PASS.
- [ ] **Step 4: Commit** `git add ... && git commit -m "docs: oss polish and demo docs"`

**Oracle gate 3:** numbers honest, repo linkable? Evidence: `bench/results/`, drill log, README demo reproduced from clone.

---

## Self-Review

**Spec coverage:** §5 dual transport → T2/T3; §6 gateway/engine/dashboard → T2–T7; §7 faults → T3 (offset replay), T5 (degraded), T10 (kill-9); §8 enterprise → T5/T11 docs; §9 verification → T8 gate + per-task tests; §10 stress → T9; §13 acceptance → T1 (CI), T8 (5-min), T9 (bench), T10 (recovery), T11 (OSS wording). Phase gates match spec §11.

**Placeholder scan:** every step names exact files, flags, env vars, and expected outputs; loader/HTML are "write completely" with defined shapes, not sketches. Fixed: cross-references use concrete names (`settle.rs`, `bench-latest.json`).

**Type consistency:** `Sink.Write(typ byte, sagaID [16]byte, payload []byte)` matches existing `RingWriter.Write` call sites; `ErrSinkFull = ErrRingFull` keeps 503 mapping; `TailEvent{typ: u8, saga_id: [u8;16], payload: Vec<u8>}` matches `SagaStore.apply` args; `SettlementRow` amounts `i64` match engine `Payload.amount: i64`; CSV `amount/fee` in minor units, gateway floats stay at edge (matcher compares cents — seed generator emits integers; gateway `transferPayload.amount` float64 → engine parses as i64 via `amount as i64`? NOTE for implementer: engine Payload.amount is already i64 and serde_json will FAIL on `100.5`; gateway must emit integer amounts — enforce in handler: reject non-integer `amount` with 400. Add that validation in T2 step 5.)

---

Plan complete and saved to `docs/superpowers/plans/2026-10-01-settleledger-showcase.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
