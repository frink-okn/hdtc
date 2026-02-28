# syntax=docker/dockerfile:1

# Build stage
FROM rust:1.93-slim AS builder

WORKDIR /build

# Install system dependencies needed by native crates (bzip2, xz)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libbz2-dev \
    liblzma-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libbz2-1.0 \
    liblzma5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hdtc /usr/local/bin/hdtc

# Default working directory for mounted data
WORKDIR /data

ENTRYPOINT ["hdtc"]
