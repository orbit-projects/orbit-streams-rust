# Orbit Streams Rust

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

## Build and test

Requires Rust 1.85+, Cargo, and `protoc`.

```sh
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release --locked --example append_plugin
```

`cargo run --release --example append_plugin` starts a Core process plugin. Its configuration is
`{"suffix":"..."}`. A Rust application can implement `BatchProcessor` and pass it to
`orbit_streams_rust::plugin::run`. The demo refuses values that would cross the shared 256 KiB
record limit.

To compare equivalent Python and Rust transforms through the real Core host, install `orbit-core`
with `process-plugins`, install `orbit-streams`, build the example in release mode, then run:

```sh
python benchmarks/compare_runtimes.py \
  --rust-plugin "$(pwd)/target/release/examples/append_plugin" \
  --records 256 --payload-bytes 128 --iterations 500 --concurrency 16
```

The benchmark also accepts `--operation checksum --rounds 10` to compare a deterministic CPU-bound
32-bit rolling checksum in both languages. `suffix` measures a cheap transform where process and
serialization costs are expected to matter more.

To run the Core-host conformance test from a workspace with `orbit-core`, `orbit-streams`, and this
repository as siblings:

```sh
python -m pip install -e "../orbit-core[process-plugins]" -e "../orbit-streams[process,dev]"
cargo build --release --locked --example append_plugin
ORBIT_STREAMS_RUST_PLUGIN="$(pwd)/target/release/examples/append_plugin" \
  python -m pytest -q ../orbit-streams/tests/test_process_rust.py
```

The harness reports Rust process startup separately, then batch p50/p95 and record throughput for
Python in-process versus Rust through Core gRPC. `--concurrency` is bounded to the process host's
default 16 calls. Repeat runs on a quiet machine and vary concurrency, record count, payload size,
and operation; retain the environment and command with any published result.
This is a local microbenchmark and does not represent broker throughput, multi-process contention,
or a production workload.

## Performance and safety

Rust can reduce per-record transformation overhead for suitable CPU-bound processors. The local
gRPC call, Protobuf encoding, copying, process scheduling, and Python conversion also cost time;
this SDK does not claim that Rust is faster end to end. Benchmark with the application's batch size,
payload distribution, transformation, process lifetime, and serialization included. Avoid sending
secrets through record representations or logs. Processor code has the host application's OS
privileges.

The capability schema is mirrored from `orbit-streams/proto/orbit/streams/v1/stream_processing.proto`;
the lifecycle schema is mirrored from Core's `process_plugin.proto`. Generated code is built from
the checked-in schemas. Protocol compatibility, Python/Rust conformance, hosted CI, supply-chain
provenance, and representative performance characterization remain release gates. This is a
pre-release implementation, not a production certification.

Licensed under Apache-2.0.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

