# Proto contracts

## Files

- `ledger.proto` — gRPC `Reconciliation` service (`GetSagaStatus`).
- `ipc-wire.md` — normative shm ring wire contract (slot layout, magic `0x534C5257`, version `1`, ordering). This is the source of truth for Go ↔ Rust.

## Regenerate Go stubs

Requires `protoc` >= 3.12, `protoc-gen-go` and `protoc-gen-go-grpc`:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc --go_out=gateway --go_opt=paths=source_relative \
       --go-grpc_out=gateway --go-grpc_opt=paths=source_relative \
       -I proto proto/ledger.proto
# generated files are committed under gateway/proto/ or gateway/internal/proto/ per module layout
```

## Regenerate Rust stubs

Rust uses `tonic_build` at compile time — no manual step. `engine/build.rs` runs:

```rust
tonic_build::compile_protos("proto/ledger.proto").unwrap();
```

Requires `protoc` on PATH. Rebuild with `cargo build` after editing `ledger.proto`.

## Wire contract

Do not hand-edit generated stubs to fix wire mismatches. Fix `ipc-wire.md` and both `wire.go` / `wire.rs` to match the fixture in `ipc-wire.md`.
