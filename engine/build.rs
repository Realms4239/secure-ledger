fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Path relative to crate root: proto lives at repo level
    tonic_build::compile_protos("../proto/ledger.proto")?;
    Ok(())
}
