fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/orbit/plugin/v1/process_plugin.proto");
    println!("cargo:rerun-if-changed=proto/orbit/streams/v1/stream_processing.proto");
    tonic_prost_build::configure()
        .build_transport(false)
        .compile_protos(
            &[
                "proto/orbit/plugin/v1/process_plugin.proto",
                "proto/orbit/streams/v1/stream_processing.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
