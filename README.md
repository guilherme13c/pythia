# Pythia

<!--toc:start-->
- [Pythia](#pythia)
  - [Tech Stack](#tech-stack)
  - [How it works](#how-it-works)
  - [Getting Started](#getting-started)
    - [1. Clone and setup](#1-clone-and-setup)
    - [2. Running Locally](#2-running-locally)
    - [3. Build the Docker Image](#3-build-the-docker-image)
    - [4. Deploy to Kubernetes](#4-deploy-to-kubernetes)
    - [5. Search](#5-search)
    - [6. Scaling](#6-scaling)
  - [Contributing](#contributing)
  - [Development](#development)
    - [Running Tests](#running-tests)
    - [Benchmarking](#benchmarking)
    - [Profiling](#profiling)
    - [Async Profiling (Tokio Console)](#async-profiling-tokio-console)
      - [Install the console tool](#install-the-console-tool)
      - [Run Pythia in profiling mode](#run-pythia-in-profiling-mode)
      - [Connect the console](#connect-the-console)
      - [CPU Profiling (Flamegraphs)](#cpu-profiling-flamegraphs)
    - [Performance Configuration](#performance-configuration)
<!--toc:end-->

A distributed, Kubernetes-native search engine and web crawler written in Rust.

I'm building Pythia to experiment with the Actor model in Rust across a
distributed network. It crawls the web, computes vector embeddings locally,
dynamically routes data across shards, and serves semantic search results
via a REST API.

## Tech Stack

- **Concurrency:** [`ractor`](https://github.com/slawlor/ractor) for
  Erlang-style actors.
- **Vector DB:** [`lancedb`](https://github.com/lancedb/lancedb) for fast, on-disk
  nearest-neighbor search.
- **Embeddings:** [`fastembed`](https://github.com/Anush008/fastembed-rs) to run
  ML models locally (no API keys needed).
- **HTTP:** [`axum`](https://github.com/tokio-rs/axum) for the search endpoint.
- **Clustering:** ractor-cluster for dynamic peer
  discovery and network message passing.
- **Deployment:** Docker & Kubernetes for seamless scaling
  of stateless and stateful actors.

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

### 1. Clone and Setup

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

### 2. Running Locally

To test the cluster locally without Kubernetes, you will need to start the seed
node (Query) first, and then attach the other actors in separate terminals:

1. `cargo run --bin query`
2. `cargo run --bin processor`
3. `cargo run --bin indexer`
4. `cargo run --bin crawler`

### 3. Build the Docker Image

Pythia uses a multi-stage Docker build to cache Rust dependencies and
pre-download the ML models.

```bash
docker build -t pythia:v1 .
```

**Note:** If you are using local Kubernetes like Minikube or Kind, make sure to
load the image into your cluster (e.g., minikube image load pythia:v1).

### 4. Deploy to Kubernetes

Spin up the entire cluster (Query API, Processors, Indexers, and Crawlers)
using the provided manifest:

```bash
kubectl apply -f pythia-cluster.yaml
```

You can watch the actors discover each other and boot up:

```bash
kubectl get pods -w
```

### 5. Search

Once the `query` pod is running, map its port to your local machine:

```bash
kubectl port-forward svc/query-service 3000:3000
```

Now you can query your search engine!

```bash
curl "http://127.0.0.1:3000/search?q=What+is+Rust&limit=5"
```

### 6. Scaling

Want to crawl or process data faster? Just open `pythia-cluster.yaml`, change
the replicas count for the crawler, indexer, or processor, and run `kubectl
apply -f pythia-cluster.yaml` again. The new nodes will instantly join the
cluster and start taking on work!

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
