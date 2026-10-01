test:
	cd gateway && go vet ./... && go test ./...
	cd engine && cargo clippy -- -D warnings && cargo test
demo:
	powershell -ExecutionPolicy Bypass -File scripts/showcase.ps1
recovery:
	powershell -ExecutionPolicy Bypass -File scripts/recovery.ps1
