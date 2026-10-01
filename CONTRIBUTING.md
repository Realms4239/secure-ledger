# Contributing

Small, verified slices beat big rewrites. Every feature goes
spec → plan → implementation (see `docs/superpowers/`), and every change
must arrive green:

- Go (`gateway/`): `go vet ./...`, `go build ./...`, `go test ./...`
- Rust (`engine/`): `cargo build`, `cargo clippy -- -D warnings`, `cargo test`
- Windows gates: `powershell -ExecutionPolicy Bypass -File scripts/showcase.ps1`
  (full demo + match proof), `scripts/recovery.ps1` (kill-9 drill)
- Cross-language contract: `proto/ipc-wire.md` is normative — shm code and
  fixtures must match it byte-for-byte; never "fix" one side silently

Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). Never
commit secrets (`.env*`, `*.key`, `*.pem`) or generated dirs (`target/`,
`bin/`, `data/*.log`). No metric claims without a committed log under
`bench/results/<date>/`. No `TODO`/`TBD` in code — file an issue instead.
