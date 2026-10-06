# Orbit Streams Rust: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

- `ORBIT_STREAMS_RUST_PLUGIN`

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

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

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
