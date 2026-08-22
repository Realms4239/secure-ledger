package status

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ledgerpb "secureledger/gateway/proto"
)

// GrpcStatus implements handler.StatusClient over the engine's gRPC service.
type GrpcStatus struct {
	client ledgerpb.ReconciliationClient
	conn   *grpc.ClientConn
}

func New(addr string) (*GrpcStatus, error) {
	// ponytail: insecure transport for slice #1 — mTLS lands with slice #2 hardening
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GrpcStatus{
		client: ledgerpb.NewReconciliationClient(conn),
		conn:   conn,
	}, nil
}

func (g *GrpcStatus) GetSagaStatus(ctx context.Context, sagaID string) (string, []string, string, error) {
	resp, err := g.client.GetSagaStatus(ctx, &ledgerpb.GetSagaStatusRequest{SagaId: sagaID})
	if err != nil {
		return "", nil, "", err
	}
	return resp.State, resp.Steps, resp.UpdatedAt, nil
}

func (g *GrpcStatus) Close() error {
	return g.conn.Close()
}
