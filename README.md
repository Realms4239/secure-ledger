# Secure Ledger — Settlement Reconciliation Prototype

> **Experimental prototype / reference architecture / simulation tool.** Not for production
> financial transactions. Not independently audited for security/compliance/correctness.
> Do not handle real money. See `docs/limitations.md`.

Go gateway (JWT + Idempotency-Key + single policy → shm ring) + Rust engine (ring reader → saga + WAL → gRPC status). Shm ring is the WSL2/Linux co-located benchmark path only (256 MB ≈100s @10k RPS, 24h≈221 GB — short buffer); production path is Postgres outbox / queue / SFTP adapters.

## 5-minute demo (WSL2)

```bash
git clone <repo> && cd secure-ledger
make demo   # builds gateway+engine, runs scripts/tracer.sh
```

Requires Go >=1.26, Rust stable via rustup, `protoc` for gRPC codegen. Runtime shm + integration (`scripts/tracer.sh`) is WSL2/Linux only; Windows is editor-only.

- `make test` — `go vet` + `go test` and `cargo clippy` + `cargo test` (Windows-safe).
- `make demo` — `bash scripts/tracer.sh` (WSL2/Linux only).

See `proto/ipc-wire.md` for the normative wire contract and `proto/README.md` for stub regeneration.
