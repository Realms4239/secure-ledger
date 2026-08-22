package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"secureledger/gateway/internal/audit"
	"secureledger/gateway/internal/handler"
	"secureledger/gateway/internal/idempotency"
	"secureledger/gateway/internal/ipc"
)

// stubStatus satisfies handler.StatusClient until Task 7 wires the real gRPC client.
type stubStatus struct{}

func (stubStatus) GetSagaStatus(context.Context, string) (string, []string, string, error) {
	return "", nil, "", errors.New("engine gRPC client not wired until task 7")
}

const defaultRingSlots = 1 << 20 // 1M slots = 256MB, per plan

func main() {
	log.SetFlags(0)
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		log.Fatal("JWT_SECRET env required")
	}
	engineAddr := os.Getenv("ENGINE_ADDR")
	if engineAddr == "" {
		engineAddr = "localhost:50051"
	}
	ringPath := os.Getenv("RING_PATH")
	if ringPath == "" {
		// ponytail: Windows dev has no /dev/shm — fall back to temp dir; WSL2/Linux keeps the shm bench path
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

	mux := http.NewServeMux()
	handler.NewTransfersHandler([]byte(secret), idempotency.New(), ring, audit.New(os.Stdout), stubStatus{}).Register(mux)

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("gateway listening on :8080 engine=%s ring=%s", engineAddr, ringPath)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	select {
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}
}
