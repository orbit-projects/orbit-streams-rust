"""Compare equivalent Python in-process and Rust process stream transforms."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from orbit.plugins.metadata import PluginMetadata
from orbit.plugins.process import ProcessPlugin
from orbit_streams import (
    MAX_BATCH_BYTES,
    ProcessStreamProcessor,
    StreamBatch,
    StreamPipeline,
    StreamRecord,
)


class PythonSuffix:
    """Bounded transforms matching the Rust example plugin."""

    def __init__(self, suffix: bytes, operation: str, rounds: int) -> None:
        self.suffix = suffix
        self.operation = operation
        self.rounds = rounds

    def _transform(self, value: bytes) -> bytes:
        if self.operation == "suffix":
            return value + self.suffix
        checksum = 2_166_136_261
        for _ in range(self.rounds):
            for byte in value:
                checksum = ((checksum ^ byte) * 16_777_619) & 0xFFFF_FFFF
        return value + checksum.to_bytes(4, "big")

    async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
        return tuple(
            StreamRecord(
                record_id=record.record_id,
                value=self._transform(record.value),
                timestamp_ms=record.timestamp_ms,
                key=record.key,
                content_type=record.content_type,
                headers=record.headers,
            )
            for record in records
        )


def percentile(samples: list[float], percent: float) -> float:
    """Return nearest-rank percentile in milliseconds."""
    ordered = sorted(samples)
    index = max(0, min(len(ordered) - 1, int((percent / 100) * len(ordered) + 0.999999) - 1))
    return ordered[index] * 1000


async def measure(
    pipeline: StreamPipeline, batch: StreamBatch, iterations: int, concurrency: int
) -> dict[str, float]:
    """Measure bounded concurrent batches including any Rust process overhead."""
    for offset in range(0, min(iterations, 20), concurrency):
        await asyncio.gather(
            *(pipeline.process(batch) for _ in range(min(concurrency, 20 - offset)))
        )
    samples: list[float] = []
    elapsed_started = time.perf_counter()
    for offset in range(0, iterations, concurrency):
        count = min(concurrency, iterations - offset)

        async def sample() -> float:
            started = time.perf_counter()
            await pipeline.process(batch)
            return time.perf_counter() - started

        samples.extend(await asyncio.gather(*(sample() for _ in range(count))))
    elapsed = time.perf_counter() - elapsed_started
    return {
        "batch_p50_ms": statistics.median(samples) * 1000,
        "batch_p95_ms": percentile(samples, 95),
        "records_per_second": len(batch.records) * iterations / elapsed,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-plugin", type=Path, required=True)
    parser.add_argument("--records", type=int, default=256)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--operation", choices=("suffix", "checksum"), default="suffix")
    parser.add_argument("--rounds", type=int, default=1)
    args = parser.parse_args()
    if not args.rust_plugin.is_absolute() or not args.rust_plugin.is_file():
        parser.error("--rust-plugin must name an existing absolute executable path")
    if not 1 <= args.records <= 1024:
        parser.error("--records must be between 1 and 1024")
    if not 0 <= args.payload_bytes <= 256 * 1024 - 6:
        parser.error("--payload-bytes must fit with the six-byte transform suffix")
    if not 10 <= args.iterations <= 100_000:
        parser.error("--iterations must be between 10 and 100000")
    if not 1 <= args.concurrency <= 16:
        parser.error("--concurrency must be between 1 and 16")
    if not 1 <= args.rounds <= 100_000:
        parser.error("--rounds must be between 1 and 100000")

    batch = StreamBatch(
        tuple(
            StreamRecord(f"event-{index}", b"x" * args.payload_bytes, index)
            for index in range(args.records)
        )
    )
    added_bytes = 6 if args.operation == "suffix" else 4
    if batch.byte_size + added_bytes * len(batch.records) > MAX_BATCH_BYTES:
        parser.error("input plus transformed bytes must fit the 1 MiB batch limit")
    python_pipeline = StreamPipeline([PythonSuffix(b"-rust", args.operation, args.rounds)])
    python_result = await measure(python_pipeline, batch, args.iterations, args.concurrency)

    plugin = ProcessPlugin(
        PluginMetadata(
            name="orbit-streams-rust",
            version="0.1.0-alpha.1",
            capabilities=frozenset({"orbit.streams"}),
        ),
        (str(args.rust_plugin),),
        configuration_json=json.dumps(
            {"operation": args.operation, "suffix": "-rust", "rounds": args.rounds}
        ).encode(),
        startup_timeout=10,
        command_timeout=10,
        shutdown_timeout=3,
    )
    startup_started = time.perf_counter()
    await plugin.activate()
    startup_ms = (time.perf_counter() - startup_started) * 1000
    processor = ProcessStreamProcessor(plugin, request_timeout=10)
    try:
        rust_pipeline = StreamPipeline([processor])
        rust_result = await measure(rust_pipeline, batch, args.iterations, args.concurrency)
        assert await rust_pipeline.process(batch) == await python_pipeline.process(batch)
    finally:
        await plugin.deactivate()

    print(
        json.dumps(
            {
                "records_per_batch": args.records,
                "payload_bytes_per_record": args.payload_bytes,
                "iterations": args.iterations,
                "concurrency": args.concurrency,
                "operation": args.operation,
                "rounds": args.rounds if args.operation == "checksum" else 0,
                "rust_process_startup_ms": round(startup_ms, 3),
                "python_in_process": {
                    key: round(value, 3) for key, value in python_result.items()
                },
                "rust_through_core_grpc": {
                    key: round(value, 3) for key, value in rust_result.items()
                },
                "rust_over_python_throughput_ratio": round(
                    rust_result["records_per_second"] / python_result["records_per_second"], 3
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
