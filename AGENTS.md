# Agent Workflow Rules — Secure Ledger (Rust-gated Go API platform)

Polyglot project: **Go API Gateway** (zero-trust edge) + **Rust Reconciliation Engine** (saga/integrity core). Vision doc: `RUSTGO.md`. Dev machine: Windows / PowerShell 5.1.

## Repo state & bootstrap

- Repo starts as two docs (`RUSTGO.md` + PDF). First working session must:
  1. `git init` (default branch `main`), commit the vision docs as the initial commit.
  2. Scaffold layout only when a plan calls for it: `gateway/` (Go module), `engine/` (Rust crate), `proto/` (shared .proto contracts).
- Toolchain prerequisites: Go ≥1.26 ✓, git ✓. **Rust (rustup) not installed** — install before any engine work. `protoc` needed once gRPC contracts land.
- Runtime-target caveat: the vision's shared-memory IPC (`shmipc`, ring buffers) is POSIX-first. On Windows dev, either target WSL2/Linux for the engine+IPC path or pick a cross-platform mechanism. Decide during design — do not silently assume either.

## Isolation first — mandatory for any code change

1. Already isolated? (`git rev-parse --git-dir` != `git rev-parse --git-common-dir`) → work where you are, skip creation.
2. Otherwise create a worktree before editing anything:
   ```
   git worktree add .worktrees/<task-slug> -b agent/<task-slug>
   cd .worktrees/<task-slug>
   ```
3. Work only inside the worktree. NEVER checkout or switch branches in the main checkout — the user edits live there.
4. Deps are cached by toolchain (`go mod`, cargo registry); no per-worktree installs needed.

## Branch & commit discipline

- Never commit directly to `main` (exception: trivial `chore:` config like `.gitignore`/`AGENTS.md`/docs-only commits during planning).
- Conventional commits: `feat:` / `fix:` / `refactor:` / `docs:` / `test:` / `chore:`
- Never commit secrets (`.env*`, keys, pats — keep them in `.gitignore` from day one) or generated dirs (`target/`, `dist/`, `out/`, `bin/`).
- Commit scope: only files relevant to the task.

## Planning workflow (deepwork)

- Long discovery phases are expected. Every feature goes: spec → plan → implementation.
- Specs: `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`. Plans: `docs/superpowers/plans/`.
- Benchmarks/metrics defined in `RUSTGO.md` (p99 <10ms @10k RPS, >99.9% match rate, RTO/RPO targets) are the acceptance bar — every spec states which metrics it serves or why it precedes them.

## Verification before done — show real output, failing checks = not done

- Go (`gateway/`): `go vet ./...`, `go build ./...`, `go test ./...`
- Rust (`engine/`): `cargo build`, `cargo clippy -- -D warnings`, `cargo test`
- Cross-cutting changes: run both sides plus the proto contract check if `proto/` changed.
- Performance claims require a recorded benchmark run, never estimates.

## Protected operations — ask before doing

- `push --force`, `reset --hard` on shared refs, branch deletion
- Dependency version bumps (go.mod, Cargo.toml workspace-wide)
- Any change to the benchmark harness or metric definitions once baselines exist
- Anything touching secrets or deploy configuration

## Session hygiene

- No session exports or summary files inside the repo. The final message reports: goal, files changed, commands run, verification results.
- On completion: merge to main, then `git worktree remove .worktrees/<slug>` and delete the merged `agent/*` branch.
