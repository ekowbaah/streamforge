# streamforge
An educational, production-engineered append-only event log in Rust. The storage core persists ordered, checksummed records; detects truncation and corruption during recovery; enforces bounded record sizes; reads bounded batches; routes keys deterministically across partitions; and tracks monotonic, consumer-group-isolated offsets. It does **not** claim Kafka protocol compatibility.

Run `cargo test`. Next milestones add segmented retention, concurrent writers, leader leases, HTTP/gRPC APIs, fault-injection tests, and an operations console.
