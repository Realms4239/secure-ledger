test:
	cd gateway && go vet ./... && go test ./...
	cd engine && cargo clippy -- -D warnings && cargo test
demo:
	bash scripts/tracer.sh
