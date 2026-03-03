# Pythia

<!--toc:start-->

- [Pythia](#pythia)
  - [Tech Stack](#tech-stack)
  - [How it works](#how-it-works)
  - [Getting Started](#getting-started)
    - [1. Clone and setup](#1-clone-and-setup)
    - [2. Run it](#2-run-it)
    - [3. Search](#3-search)
  - [Contributing](#contributing)
  - [Development](#development) - [Running Tests](#running-tests) - [Benchmarking](#benchmarking) - [Profiling](#profiling) - [Async Profiling (Tokio Console)](#async-profiling-tokio-console) - [Install the console tool](#install-the-console-tool) - [Run Pythia in profiling mode](#run-pythia-in-profiling-mode) - [Connect the console](#connect-the-console) - [CPU Profiling (Flamegraphs)](#cpu-profiling-flamegraphs) - [Performance Configuration](#performance-configuration)
  <!--toc:end-->

A concurrent, local search engine and web crawler written in Rust.

I'm building Pythia to experiment with the Actor model in Rust. It
crawls the web, extracts text, computes vector embeddings locally,
and serves semantic search results via a REST API.

## Tech Stack

- **Concurrency:** [`ractor`](https://github.com/slawlor/ractor) for
  Erlang-style actors.
- **Vector DB:** [`lancedb`](https://github.com/lancedb/lancedb) for fast, on-disk
  nearest-neighbor search.
- **Embeddings:** [`fastembed`](https://github.com/Anush008/fastembed-rs) to run
  ML models locally (no API keys needed).
- **HTTP:** [`axum`](https://github.com/tokio-rs/axum) for the search endpoint.

## How it works

The architecture relies on message passing between a few specific actors:

- **Managers & Workers:** Handle the crawling queue, respect `robots.txt`,
  and scrape HTML.
- **Processor:** Cleans and chunks massive text blobs.
- **Indexer:** Computes the embeddings and saves them to LanceDB.
- **Query:** Takes HTTP requests, embeds the query string, and fetches results.

## Getting Started

You'll need a recent Rust toolchain and the protobuf compiler
(`sudo apt install protobuf-compiler` or `brew install protobuf` for LanceDB).

### 1. Clone and setup

```bash
git clone https://github.com/guilherme13c/pythia.git
cd pythia
```

Create a `seeds.txt` file in the root directory to give the crawler a starting point:

```plaintext
https://en.wikipedia.org/
https://rust-lang.org/
```

You can optionally create a .env file to tweak settings (see `src/config.rs`
for defaults).

### 2. Run it

```bash
cargo run --release
```

The crawler will start running immediately, and the Axum server will bind to `127.0.0.1:3000`.

### 3. Search

```bash
curl "http://127.0.0.1:3000/search?q=What+is+Rust&limit=5"
```

---

## Contributing

We're actively looking for contributors! There is a bunch of open issues (some
very beginner-friendly) on the tracker. See `CONTRIBUTING.md` for details on
how to jump in.

---

## Development

### Running Tests

Pythia uses standard Rust unit and integration tests. To run the full test suite:

```bash
cargo test
```

### Benchmarking

Benchmarks are managed using Criterion. These measure HTML parsing, frontier
ingestion, and scheduler efficiency.

To run all benchmarks:

```bash
cargo bench
```

Note: Results and flamegraphs (if configured) will be generated in `target/criterion`.

### Profiling

Pythia is instrumented for both async task monitoring and CPU profiling.

### Async Profiling (Tokio Console)

We use tokio-console to monitor actor execution and detect task starvation.

#### Install the console tool

```bash
cargo install tokio-console
```

#### Run Pythia in profiling mode

We have configured a custom alias to run with the necessary unstable flags
and an isolated target directory:

```bash
cargo console
```

#### Connect the console

In a separate terminal, run:

```bash
tokio-console
```

#### CPU Profiling (Flamegraphs)

CPU profiling is integrated into the benchmark suite via pprof.
Running cargo bench automatically generates flamegraphs in the benchmark
output directory.

### Performance Configuration

To avoid constant recompilation when switching between development and
profiling, the project uses a dedicated target directory for console
builds defined in `.cargo/config.toml`:

- Default Target: `target/`
- Console Target: `target/tokio-console`
