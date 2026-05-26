# syntax=docker/dockerfile:1.7
#
# Multi-stage build for both binaries (ingest-daemon and api-server).
# Uses cargo-chef so dependency compilation is cached independently of
# source changes — subsequent builds typically rebuild in under a minute.
#
# Build targets:
#   docker build --target ingest --tag market-data-ingest .
#   docker build --target api    --tag market-data-api    .
#
# Coolify picks the target via the `target` key in docker-compose.yml.

# ─── Stage 1: chef base ─────────────────────────────────────────────────
FROM rust:1.93-slim-bookworm AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked --version 0.1.68

# ─── Stage 2: plan dependencies ────────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ─── Stage 3: build dependencies (cached layer) ────────────────────────
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# ─── Stage 4: build the workspace ──────────────────────────────────────
COPY . .
RUN cargo build --release --bin ingest-daemon --bin api-server

# ─── Stage 5: ingest-daemon runtime image ──────────────────────────────
FROM debian:bookworm-slim AS ingest
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 ingest \
    && useradd  --system --uid 10001 --gid ingest --shell /usr/sbin/nologin ingest \
    && mkdir -p /var/data/raw && chown -R ingest:ingest /var/data
COPY --from=builder /app/target/release/ingest-daemon /usr/local/bin/ingest-daemon
USER ingest
# Metrics port — Coolify health-checks via this endpoint.
EXPOSE 9100
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://127.0.0.1:9100/metrics || exit 1
ENTRYPOINT ["/usr/local/bin/ingest-daemon"]

# ─── Stage 6: api-server runtime image ─────────────────────────────────
FROM debian:bookworm-slim AS api
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10002 api \
    && useradd  --system --uid 10002 --gid api --shell /usr/sbin/nologin api
COPY --from=builder /app/target/release/api-server /usr/local/bin/api-server
USER api
EXPOSE 8080 9101
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://127.0.0.1:8080/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/api-server"]
