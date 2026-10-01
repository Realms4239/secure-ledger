package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"secureledger/gateway/internal/audit"
	"secureledger/gateway/internal/dash"
	"secureledger/gateway/internal/fraud"
	"secureledger/gateway/internal/handler"
	"secureledger/gateway/internal/idempotency"
	"secureledger/gateway/internal/ipc"
	"secureledger/gateway/internal/ratelimit"
	"secureledger/gateway/internal/status"
)

const defaultRingSlots = 1 << 20 // 1M slots = 256MB, per plan

func rateLimitRPS() float64 {
	if v := os.Getenv("RATE_LIMIT_RPS"); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%f", &f); err == nil && f > 0 {
			return f
		}
	}
	return 100
}

func rateLimitBurst() int {
	if v := os.Getenv("RATE_LIMIT_BURST"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return 200
}

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
	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		// Loopback by default: privileged routes (chaos, fail-inject) are
		// JWT-only, so never bind all interfaces without deciding to.
		gatewayAddr = "127.0.0.1:8080"
	}
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
		ringPath = os.Getenv("RING_PATH")
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
		sink = ring
		log.Printf("gateway transport=shm ring=%s", ringPath)
	}

	grpcStatus, err := status.New(engineAddr)
	if err != nil {
		log.Fatalf("engine gRPC client %s: %v", engineAddr, err)
	}
	defer grpcStatus.Close()

	settlementDir := os.Getenv("SETTLEMENT_DIR")
	if settlementDir == "" {
		settlementDir = "data/settlement"
	}
	reportPath := os.Getenv("REPORT_PATH")
	if reportPath == "" {
		reportPath = "data/match-report.json"
	}

	partitionFlag := os.Getenv("PARTITION_FLAG")
	if partitionFlag == "" {
		partitionFlag = "data/partition.flag"
	}

	mux := http.NewServeMux()
	hub := dash.NewHub()
	hub.Register(mux)
	transfers := handler.NewTransfersHandler([]byte(secret), idempotency.New(), sink, audit.New(os.Stdout), grpcStatus, fraud.New(), settlementDir, reportPath)
	transfers.SetHub(hub)
	transfers.SetPartitionFlag(partitionFlag)
	if os.Getenv("ENABLE_FAIL_INJECT") == "1" {
		transfers.SetFailInject(true)
		log.Print("gateway WARNING: fail-injection route enabled (test only)")
	}
	transfers.SetRateLimiter(ratelimit.New(rateLimitRPS(), rateLimitBurst()))
	transfers.Register(mux)

	// No WriteTimeout: SSE streams live indefinitely. Read/Idle bound the
	// slow-loris surface instead; per-route write deadlines are future work.
	srv := &http.Server{
		Addr: gatewayAddr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("gateway listening on %s engine=%s", gatewayAddr, engineAddr)

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
