# Pythia

<!--toc:start-->

- [Pythia](#pythia)
  - [Architecture](#architecture)
  - [Tech Stack](#tech-stack)
  - [Getting Started (Local Development)](#getting-started-local-development)
  - [Development & Testing](#development-testing)
    - [Running Tests](#running-tests)
    - [Benchmarking](#benchmarking)
    - [Configuration (`.env`)](#configuration-env)
    - [Docker & Kubernetes (Coming Soon)](#docker-kubernetes-coming-soon)
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

1. Prerequisites
   You will need Rust and Docker installed on your machine.

2. Start the Message Broker
   Pythia requires RabbitMQ to route messages between the microservices. Spin up
   a local instance using Docker:

```
bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

(You can view the RabbitMQ management UI at <http://localhost:15672> with guest/guest).

1. Pre-download AI Models
   Before starting the services, download the gigabytes of embedding and reranking
   models to your local cache:

```bash
cargo run --bin download_models -p processor 4. Run the Microservices
```

Because Pythia is a distributed system, you need to run the services
concurrently. Open four separate terminal tabs and run:

```bash
RUST_LOG=info cargo run -p crawler
RUST_LOG=info cargo run -p processor
RUST_LOG=info cargo run -p indexer
RUST_LOG=info cargo run -p query
```

1. Perform a Search
   Once the crawler has ingested some data, you can query the system via
   the REST API:

```bash
curl "<http://localhost:4000/search?q=your+search+query&limit=5>"
```

## Development & Testing

### Running Tests

Pythia uses standard Rust unit tests and a workspace-level End-to-End (E2E)
integration test that spins up all microservices and a mock web server.

```bash
# Run all unit tests
cargo test

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

### Configuration (`.env`)

Environment variables are managed via `dotenvy`. A root `.env` file provides
shared variables (like `AMQP_ADDR`, `RUST_LOG`, and internal routing URLs),
while service-specific `.env` files define individual ports and scaling
factors (e.g., `CRAWLER_SHARDS`).

### Docker & Kubernetes (Coming Soon)

The migration to microservices was done specifically to support container
orchestration. Dockerfiles and Helm charts/Kubernetes manifests for
deploying Pythia to a distributed cluster are currently under
active development!

## Contributing

Contributions are welcome! Please check the issues page (look for good first issue if you're new to the codebase). See `CONTRIBUTING.md` for details on how to jump in.
