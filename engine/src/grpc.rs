// Generated protobuf/gRPC types for secureledger.v1.
// The tonic service impl lives in main.rs (GrpcBridge) so it can share the
// same Arc'd SagaStore as the ring consumer — one source of truth in-process.
#[allow(clippy::result_large_err)] // tonic-generated signatures return large Status by design
pub mod pb {
    tonic::include_proto!("secureledger.v1");
}
