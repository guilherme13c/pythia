# ==========================================
# STAGE 1: The Builder (Compiles the code)
# ==========================================
FROM ubuntu:24.04 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
  curl \
  protobuf-compiler \
  pkg-config \
  libssl-dev \
  build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install Rust directly from the official installer (latest version)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

ENV RUSTFLAGS="--cfg tokio_unstable"

# 1. Copy only the dependency manifests
COPY Cargo.toml Cargo.lock ./

# 2. Create dummy source files to trick Cargo into building ONLY the dependencies
RUN mkdir -p src/bin benches && \
  echo "pub fn dummy() {}" > src/lib.rs && \
  echo "fn main() {}" > src/bin/crawler.rs && \
  echo "fn main() {}" > src/bin/indexer.rs && \
  echo "fn main() {}" > src/bin/processor.rs && \
  echo "fn main() {}" > src/bin/query.rs && \
  echo "fn main() {}" > src/bin/download_models.rs && \
  echo "fn main() {}" > benches/crawler.rs && \
  echo "fn main() {}" > benches/indexer.rs && \
  echo "fn main() {}" > benches/query.rs && \
  echo "fn main() {}" > benches/processor.rs

# This step will take a long time the FIRST time, but will be instantly cached for future builds
RUN cargo build --release

# 3. Now copy your ACTUAL source code
COPY src ./src

# 4. Touch the files to update their timestamps. This forces Cargo to rebuild YOUR code, 
# but it will reuse the cached dependencies from step 2.
RUN touch src/lib.rs src/bin/*.rs
RUN cargo build --release

# 5. Pre-download the AI models so they are baked into the image
ENV FASTEMBED_CACHE_PATH=/app/models
RUN mkdir -p /app/models
RUN ./target/release/download_models


# ==========================================
# STAGE 2: The Runtime (Small final image)
# ==========================================
FROM ubuntu:24.04

WORKDIR /app

# Install required system dependencies (OpenSSL is needed for reqwest/HTTPS crawling)
RUN apt-get update && \
  apt-get install -y ca-certificates libssl-dev && \
  rm -rf /var/lib/apt/lists/*

# Copy the compiled binaries from the builder stage
COPY --from=builder /app/target/release/crawler /app/
COPY --from=builder /app/target/release/indexer /app/
COPY --from=builder /app/target/release/processor /app/
COPY --from=builder /app/target/release/query /app/

# Copy the seeds file
COPY seeds.txt /app/seeds.txt

# Copy the pre-downloaded AI models
COPY --from=builder /app/models /app/models
ENV FASTEMBED_CACHE_PATH=/app/models

# Create the data directory for LanceDB and SQLite
RUN mkdir -p /app/data

# Expose the API port (3000) and the Ractor Cluster port (8000)
EXPOSE 3000 8000

# We don't set an ENTRYPOINT here because Kubernetes will specify 
# which binary (crawler, indexer, etc.) to run for each specific pod.
