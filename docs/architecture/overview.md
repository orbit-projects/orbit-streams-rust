# Orbit Streams Rust: architecture and boundaries

## Responsibility

`orbit-streams-rust` is the Rust implementation of the provider-neutral stream-processing
capability also implemented by Python `orbit-streams`. It executes the application's compiled
batch processor itself inside a Core-supervised process. Choose it for CPU-heavy or memory-sensitive
transforms after comparing the complete process path; Python remains the direct in-process option.
Applications select and configure one implementation explicitly.

The implementation provides generated typed Protobuf messages, bounded batch validation, a
`BatchProcessor` contract, and a Core-supervised local gRPC runtime. It transforms each bounded
batch concurrently up to 16 calls, with backpressure beyond that bound. Core owns child lifecycle,
configuration, health, authentication, and shutdown. The process boundary is trusted same-user
loopback, not a sandbox. This package does not provide broker clients, durable delivery,
acknowledgements, checkpoints, retries, or exactly-once semantics.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `Cargo.toml`
- `prost`
- `prost-types`
- `serde`
- `serde_json`
- `tokio`
- `tokio-stream`
- `tonic`
- `tonic-prost`

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `benchmarks/compare_runtimes.py`
- `build.rs`
- `src/lib.rs`
- `src/plugin.rs`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
