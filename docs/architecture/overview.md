# Architecture

`orbit-streams-rust` is a Rust implementation of Orbit's bounded batch-processing capability. A
Core-supervised process hosts the application's compiled synchronous `BatchProcessor`, validates
Protobuf inputs and outputs, and bridges the processor to Python Core over a local gRPC stream.
Python `orbit-streams` remains the direct in-process implementation; applications select one
implementation explicitly.

```text
Orbit Core process host (Python)
  ├── launches the absolute executable path
  ├── sends one-use authentication and JSON configuration
  ├── owns lifecycle, health, deadlines, and shutdown
  └── invokes /orbit.streams.v1.BatchProcessor/Process
                 ↓ local gRPC + Protobuf
Rust plugin runtime
  ├── bounds messages and in-flight work (maximum 16)
  ├── validates StreamBatch request
  ├── calls synchronous BatchProcessor on a blocking worker
  └── validates StreamBatch response before returning it
```

The runtime uses bounded channel capacity and a semaphore for backpressure. Lifecycle commands
drain active capability calls before changing processor activation state. The processor is called
concurrently; implementations must be thread-safe and return promptly. The Core host owns the
authoritative process-call deadline.

## Data contract

The shared `orbit.streams.v1` schema is owned by the Python `orbit-streams` capability package and
mirrored here for code generation. Core's process-plugin lifecycle schema is also mirrored. Keep
generated bindings aligned with both checked-in source schemas.

The Rust SDK re-exports generated `StreamBatch` and `StreamRecord`, provides `validate_batch`, and
publishes bounded limit constants. Requests and processor output are both validated. The output
must preserve the input's declared `max_records` and `max_bytes`. Limits constrain record fields
and aggregate data; they do not implement acknowledgements, checkpoints, ordering, retries, or
exactly-once processing.

## Performance boundary

Rust can reduce processor CPU or memory overhead for some transforms. The full path includes
process startup, Python/Rust conversion, Protobuf encoding, gRPC, copies, scheduling, and result
conversion. Benchmark the whole application path and representative payloads before choosing this
implementation. The included microbenchmark is local and does not measure brokers, durable
delivery, or production contention.

## Trust boundary

Core supervises the process, but the local stream is plaintext loopback for trusted same-user
plugin code. The process boundary is not a sandbox. Processor code runs with the executable's OS
permissions; isolate filesystem, network, credentials, and resource usage at deployment time.
