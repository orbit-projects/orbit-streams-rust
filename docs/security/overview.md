# Security

## Process trust

The Rust executable is a child process supervised by Core, but the gRPC stream is plaintext
loopback for trusted same-user plugin code. Core's one-use authentication token binds the expected
session; it is not a sandbox or protection from another process running as the same OS user. Do
not execute mutually untrusted processors in this trust boundary. Use OS/container isolation,
resource limits, and restricted filesystem and network access where needed.

The executable runs with its OS identity's privileges. Any credentials available to the process
may be read by processor code. Give it only the permissions required by the workload, and avoid
placing secrets in benchmark data, record headers, values, or logs.

## Batch data

Stream records may contain personal or confidential data. The runtime bounds batches and validates
metadata but does not encrypt, redact, classify, or persist records. Do not log record values or
headers by default. Keep processor errors generic: `ProcessorFailure` carries no arbitrary text,
and capability error responses use bounded codes rather than panic details.

Validate all output before downstream publication. A processor can return malformed or sensitive
data even after receiving a valid request; the SDK verifies shape and size, not business policy,
authorization, or data-loss prevention.

## Protocol and supply chain

Bootstrap is limited in size, rejects unknown JSON fields, and accepts a loopback IPv4 endpoint and
bounded token shape. Treat the Core-hosted schema and generated code as supply-chain inputs; review
changes to both the stream capability schema and Core lifecycle schema. Pin Cargo dependencies and
build from the checked-in lockfile. A local build is not signed provenance.

## Vulnerability reporting

Report suspected vulnerabilities using the private process described in the repository
[security policy](../../SECURITY.md).
