FROM debian:trixie-slim AS builder

RUN apt-get update \
  && apt-get install -y \
  curl \
  pkg-config \
  libssl-dev \
  build-essential \
  clang \
  mold \
  protobuf-compiler

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /usr/src/pythia
COPY . .

ENV CARGO_BUILD_JOBS=2

RUN --mount=type=cache,target=/root/.cargo/registry \
  --mount=type=cache,target=/root/.cargo/git \
  --mount=type=cache,target=/usr/src/pythia/target \
  cargo build --release -p crawler -p processor -p indexer -p query && \
  mkdir -p /out && \
  cp target/release/crawler /out/ && \
  cp target/release/processor /out/ && \
  cp target/release/indexer /out/ && \
  cp target/release/query /out/

FROM nvidia/cuda:12.2.2-cudnn8-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
  ca-certificates \
  libssl3 \
  sqlite3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/crawler /app/
COPY --from=builder /out/processor /app/
COPY --from=builder /out/indexer /app/
COPY --from=builder /out/query /app/

ENV RUST_LOG=info
ENV AMQP_ADDR=amqp://rabbitmq:5672/%2f
