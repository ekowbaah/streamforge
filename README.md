# streamforge
An educational, production-engineered append-only event log in Rust. The MVP persists ordered, checksummed records and detects truncation or corruption during recovery. It does **not** claim Kafka protocol compatibility.

Run `cargo test`. Next milestones cover partitions, retention, consumer offsets, backpressure, leader leases, failure tests, HTTP/gRPC APIs, and the operations console.
