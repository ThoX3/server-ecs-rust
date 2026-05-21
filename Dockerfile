FROM rust:bookworm as builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libudev-dev \
    libwayland-dev \
    libxkbcommon-dev \
    libasound2-dev \
    clang \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    libssl-dev

COPY . .

RUN cargo build --release -j 2

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libudev1 \
    libwayland-client0 \
    libxkbcommon0 \
    libasound2 \
    libprotobuf32 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/gatekeeper .
COPY --from=builder /app/target/release/orchestrator .
COPY --from=builder /app/target/release/dedicated_server .

RUN chmod +x gatekeeper orchestrator dedicated_server
