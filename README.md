# Pythia

Lightweight Rust actor-based indexing and crawling pipeline

## Project Summary

`Pythia` is a modular, actor-oriented Rust project implementing a distributed crawling, processing, indexing, and query pipeline. It separates responsibilities across actor groups (`crawler`, `processor`, `indexer`, and `query`) so components can scale independently and be tested in isolation.

## Features

- Modular actor layout: `crawler`, `processor`, `indexer`, `query`
- Distributed-friendly design for parallel workers
- Pluggable processing pipeline for document transformation
- Efficient indexing and query flow

## Architecture Overview

The codebase is organized under `src/actors` with four main groups:

- `crawler` — fetches content and pushes jobs to the pipeline
- `processor` — transforms and extracts features from raw content
- `indexer` — stores and updates indexed representations
- `query` — serves lookup/query requests against the index

Actors communicate via in-process message types defined in the `messages` modules; see the respective actor subfolders for message schemas and state definitions.

## Repository Layout

- `src/main.rs` — application entrypoint and actor wiring
- `src/actors/` — actor groups and submodules
	- `crawler/` — crawling manager and worker implementations
	- `processor/` — processing actor and message definitions
	- `indexer/` — indexing actor and state
	- `query/` — query actor and API

## Quickstart

Build and run locally:

```bash
# Build (release for better performance)
cargo build --release

# Run the service
cargo run --release

# Run tests
cargo test
```

Enable verbose logging for local debugging:

```bash
export RUST_LOG=info
cargo run
```

## Configuration

Configuration is primarily controlled via environment variables and command-line flags (where present). Common settings:

- `RUST_LOG` — logging verbosity
- Any actor-specific configuration is defined near the actor's `state.rs` or `mod.rs` files (see `src/actors/*`)

Add a short config loader or `.env` usage if you plan to support richer runtime configuration.

## Development

- Code is idiomatic Rust — use `cargo fmt` and `cargo clippy` before submitting a PR.
- Tests live next to the modules they exercise; run `cargo test` to execute the suite.
- To add a new actor, add a submodule under `src/actors` and register it in `main.rs`.

Suggested developer commands:

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```

## Examples / Usage

Look at the actor folders for example message flows. To prototype a new pipeline step, implement a `processor` actor that accepts the existing message type and emits the transformed message consumed by `indexer`.

## Deployment

- Build with `--release`.
- Consider containerizing the binary and running multiple replicas of the `crawler` and `processor` actors behind a supervisor or orchestration layer.
- Use structured logging and a metrics exporter for observability.

## Contributing

- Fork the repo and open a pull request with a clear description and tests.
- Follow Rust formatting and linting conventions; keep changes focused and well-documented.

## Roadmap & TODOs

- Add integration tests for end-to-end crawling → indexing → query flows
- Provide a lightweight CLI for administrative tasks (index rebuild, snapshot)
- Add graceful shutdown and state checkpointing for actors

## License

This project is licensed under the terms in the `LICENSE` file.

## Maintainers

Maintained by the contributors listed in the repository. For issues or questions open an issue on the tracker.

