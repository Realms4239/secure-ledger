# Secure Ledger — Settlement Reconciliation Showcase

> **Experimental prototype / reference architecture / simulation tool.** Not for production
> financial transactions. Not independently audited for security/compliance/correctness.
> Do not handle real money. See `docs/limitations.md`.

Go gateway (JWT + Idempotency-Key + fraud rules → file/shm transport) + Rust engine
(replay DFA + settlement matcher + WAL → Postgres mirror + gRPC status) + live dashboard.
Simulated MVola/Orange/Airtel traffic; the shm ring is the WSL2/Linux co-located
benchmark path only (256 MB ≈100s @10k RPS); the file transport is the Windows demo
path and the production-plausible outbox pattern.

## 5-minute demo (Windows)

Requires Go ≥1.26, Rust stable, `protoc`. Postgres optional (engine runs degraded without it).

```powershell
git clone <repo> && cd secure-ledger
$env:JWT_SECRET = "demo-secret-32-bytes-long!!!!"
# terminal 1: engine
$env:TRANSPORT="file"; cargo run -p secureledger-engine
# terminal 2: gateway
$env:TRANSPORT="file"; $env:JWT_SECRET="demo-secret-32-bytes-long!!!!"
go run ./gateway/cmd/gateway
# terminal 3: seed, load, settle
go run ./gateway/cmd/seed -dir seed-out
go run ./gateway/cmd/loader -jwt-secret "demo-secret-32-bytes-long!!!!" -transfers seed-out/transfers.jsonl -c 50
# upload seed-out/settlement-10k.csv via POST /settlement/upload, then open:
# http://localhost:8080/dashboard
```

Or one command for the full proof: `powershell -ExecutionPolicy Bypass -File scripts/showcase.ps1`
(builds, loads 10k transfers, compares the match report byte-for-byte).
Kill-9 drill: `scripts/recovery.ps1`.

## What the numbers say

First recorded run: `bench/results/2026-10-01/` — p99 495ms @ 260 RPS on a
degraded laptop (C: disk full), match classes 100% exact on the golden set.
The RUSTGO bar (p99 <10ms @10k RPS, 24h sustained) is NOT claimed; the hooks
to measure it are in place (`cmd/loader`, `bench/`).

## Layout

- `gateway/` — Go edge: intake, authn, fraud plugin, transports, dashboard
- `engine/` — Rust core: tail/ring consumers, saga DFA, WAL, matcher, PG sink
- `proto/` — `ledger.proto` (gRPC) + `ipc-wire.md` (normative shm contract)
- `sample-data/` — golden seed (transfers + settlement CSV + expected report)
- `scripts/` — `showcase.ps1` (demo+proof gate), `recovery.ps1` (kill-9 drill)
- `bench/results/` — committed measurement logs (never estimates)
- `docs/` — `architecture.md`, `integration.md`, `limitations.md`

See `proto/ipc-wire.md` for the normative wire contract and `proto/README.md` for stub regeneration.

## Transport choice

`TRANSPORT=file` (default): JSONL append + tail with persisted offset.
Windows-native; same event schema as the ring; offset = replay position.
`TRANSPORT=shm`: the frozen WSL2 bench path (`/dev/shm/secureledger.ring`).
