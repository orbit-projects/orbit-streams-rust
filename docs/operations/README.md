# Operations

The Rust package provides a processor runtime, not a stream broker integration. It consumes one
validated batch per Core capability request and returns a transformed or filtered batch. It does
not poll brokers, acknowledge messages, persist offsets, retry failures, checkpoint state, or
guarantee ordering or exactly-once delivery. The surrounding application or a separate broker
adapter owns those semantics.

## Processor contract

Implement `BatchProcessor` and pass it to `orbit_streams_rust::plugin::run`. `activate` receives
Core's JSON configuration after the process handshake; `process` receives ownership of one
validated `StreamBatch`; `deactivate` runs before normal process shutdown. The processor is
synchronous and executes on a blocking worker thread. Calls may run concurrently up to 16, so
shared processor state must be thread-safe. Avoid unbounded blocking and coordinate long-running
work with the host's request deadlines.

Return a batch with the same `max_records` and `max_bytes` declarations as its input. Both the
input and output are validated, including identifiers, timestamp, content type, header limits,
record count, value size, and aggregate bytes. Invalid output becomes a sanitized capability error.

## Core process setup

Build an executable with a fixed absolute path. Configure Core's `ProcessPlugin` with the matching
name, `orbit.streams` capability, and JSON configuration; Core sends configuration after the
authenticated process handshake rather than through command-line arguments. The plugin reads one
bounded bootstrap record from stdin and connects only to the supplied loopback endpoint. Core owns
process startup, configuration, health, deadlines, and shutdown.

The README's `append_plugin` example accepts a `suffix` JSON setting. Its benchmark compares
Python in-process calls against Rust through the process boundary. Repeat runs on a quiet machine
and vary payload size, record count, concurrency, operation, and process startup. Treat these
results as local measurements, not broker capacity guarantees.

## Capacity and shutdown

The runtime allows up to 16 capability calls in flight and uses a bounded channel for backpressure.
Input/output batches are limited to 1,024 records, 256 KiB per value, and 1 MiB aggregate record
field data; header and identifier limits apply per record. Size upstream batches so their encoded
Protobuf representation also fits the host's message limit.

On lifecycle transitions the runtime drains active calls before activation changes. During normal
shutdown, Core owns child termination and process cleanup. Make processor deactivation bounded
and do not rely on abrupt process termination to flush application state; this package provides
no durable state or flush guarantee.

## Release validation

Run `cargo test --locked`, Clippy with warnings denied, the release build, and Python/Core
cross-language conformance. Test a dedicated Core host with representative processors for
cancellation, overload/backpressure, malformed messages, process restart, shutdown during work,
and error sanitization. Hosted CI, provenance, live deployment, and representative production
performance remain external release gates.
