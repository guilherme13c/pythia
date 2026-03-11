# Pythia

<!--toc:start-->

- [Pythia](#pythia)
  - [Architecture](#architecture)
  - [Tech Stack](#tech-stack)
  - [Getting Started (Local Development)](#getting-started-local-development)
    - [1. Prerequisites](#1-prerequisites)
    - [2. Build and Deploy](#2-build-and-deploy)
    - [3. Seed the Crawler](#3-seed-the-crawler)
    - [4. Perform a Search](#4-perform-a-search)
  - [Development & Testing](#development-testing)
    - [Running Tests](#running-tests)
    - [Benchmarking](#benchmarking)
    - [Docker & Kubernetes](#docker-kubernetes)
  - [Contributing](#contributing)
  <!--toc:end-->

A distributed, microservice-based search engine and web crawler written in Rust.

I'm building Pythia to experiment with the Actor model in Rust across a
distributed network. It crawls the web, computes vector embeddings
locally, dynamically routes data across queues, and serves semantic
search results via a REST API with Cross-Encoder Reranking.

Formerly a monolithic actor system, Pythia has been completely re-architected
into four distinct microservices, preparing it for high-availability
deployment on Kubernetes.

## Architecture

Pythia consists of four decoupled Rust microservices communicating via RabbitMQ:

- Crawler (`/services/crawler`): Uses headless Chrome and Reqwest to scrape web
  pages. Saves raw HTML to a local SQLite blob store and publishes events to RabbitMQ.

- Processor (`/services/processor`): Consumes crawler events, reads the HTML
  blob, chunks the text, and generates dense vector embeddings using FastEmbed.

- Indexer (`/services/indexer`): Consumes vector payloads and persists them into
  LanceDB (a serverless vector database) using Apache Arrow formatting.

- Query (`/services/query`): A REST API built with axum. It converts user search
  queries into vectors, queries LanceDB for nearest neighbors, and pipes the
  results back through the Processor for high-accuracy Cross-Encoder Reranking.

## Tech Stack

- Language: Rust

- Async Runtime: Tokio

- Actor Framework: Ractor

- Message Broker: RabbitMQ (lapin)

- Web Framework: Axum

- Vector Database: LanceDB & Apache Arrow

- Blob Storage: SQLite (rusqlite)

- AI Models: FastEmbed (All-MiniLM-L6-v2 + Text Rerankers)

- Logging/Observability: Tracing (tracing, tracing-subscriber)

## Getting Started (Local Development)

### 1. Prerequisites

You will need Rust, Docker, kubectl, and k3d installed on your machine.
Start by creating a local Kubernetes cluster:

```bash
k3d cluster create pythia-cluster
```

### 2. Build and Deploy

Pythia is designed to run entirely inside Kubernetes. Build the
unified Docker image and load it into your local cluster:

```bash
# Build the image
docker build -t pythia:latest .

# Import into k3d
k3d image import pythia:latest -c pythia-cluster

# Apply the cluster manifest (Storage, RabbitMQ, Browserless, and Pythia Services)
kubectl apply -f pythia-cluster.yaml
```

### 3. Seed the Crawler

The crawler relies on an SQLite-backed frontier. Once the crawler-0 pod is
running, inject your seed URLs directly into the database:

```bash
kubectl exec statefulset/crawler -- sqlite3 /data/frontier.db "INSERT OR IGNORE INTO urls (url, status) VALUES ('[https://www.rust-lang.org/](https://www.rust-lang.org/)', 'pending');"
```

The crawler will automatically wake up, fetch the page, and stream the data through the Processor and Indexer.

### 4. Perform a Search

Once the crawler has ingested and indexed some data, you can query the system via the exposed REST API (NodePort 30000):

```bash
curl "http://localhost:30000/search?q=rust+programming+language&limit=5"
```

## Development & Testing

### Running Tests

Pythia uses standard Rust unit tests and a workspace-level End-to-End (E2E)
integration test that spins up all microservices and a mock web server.

```bash
# Run all unit tests
cargo test --workspace --lib --bins

# Run the E2E Microservice Pipeline test
cargo test -p shared --test e2e_test
```

### Benchmarking

Benchmarks are managed using Criterion. They measure pure logic bottlenecks
like HTML parsing, text chunking, payload serialization, and Apache
Arrow batch construction.

To run all benchmarks across the workspace:

```bash
cargo bench
```

Note: Results and HTML performance reports will be generated in `target/criterion`.

### Docker & Kubernetes

Environment variables and scaling parameters are strictly managed via the Kubernetes manifest (`pythia-cluster.yaml`).

To scale the crawler, you can adjust the `N_DYNAMIC_WORKERS` and
`N_STATIC_WORKERS` environment variables on the _StatefulSet_,
or increase the `replicas` count to spawn entirely new Shards.

## Contributing

Contributions are welcome! Please check the issues page (look for good first
issue if you're new to the codebase). See `CONTRIBUTING.md` for details on
how to jump in.
