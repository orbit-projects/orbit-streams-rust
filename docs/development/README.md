# Orbit Streams Rust: development

Requires Rust 1.85 or later. CI runs the checks below on Rust 1.85 and stable. Use the checks for
each language manifest present in this repository:

### Rust package

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo build --release --locked --example append_plugin
```

Optional SDKs and live-service tests are described by the package README. Do not run an opt-in live test against a shared or production service unless its own documentation explicitly supports that use.

## Contribution expectations

Add focused regression coverage for behavior changes. Update the README and relevant guide when a contract, configuration option, security property, lifecycle rule, or operational assumption changes. Report the exact versions and services exercised; local checks do not imply hosted CI or release validation.

On 2026-10-10, formatting, all **4 tests**, Clippy with warnings denied, and the locked release
plugin build passed on Rust 1.85.0 and Rust 1.99.0. Python/Core cross-language conformance, hosted CI,
provenance, and representative performance characterization remain release gates.
