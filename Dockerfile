# syntax=docker/dockerfile:1
#
# Multi-stage build for xs3lerator.
# Uses glibc (gnu) targets because foundationdb-sys requires dynamic linking
# against libfdb_c.so — musl's static-pie mode cannot link shared libraries.
# The runtime container (ubuntu:24.04) provides the matching glibc.
#
# Caching strategy: dependencies are built in a separate layer from application
# code. In CI (GHA), type=gha,mode=max caches ALL intermediate layers across
# runs, so the dependency layer is only rebuilt when Cargo.toml/Cargo.lock change.

# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        libclang-dev \
        curl \
    && ARCH=$(dpkg --print-architecture) \
    && case "$ARCH" in amd64) FDB_ARCH=amd64 ;; arm64) FDB_ARCH=aarch64 ;; *) echo "unsupported: $ARCH" && exit 1 ;; esac \
    && curl -fsSL "https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_${FDB_ARCH}.deb" -o /tmp/fdb-clients.deb \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Dependency cache layer ───────────────────────────────────────────────────
# Copy only manifests + stub source so this layer is invalidated only when
# dependencies change. The compiled deps stay in /app/target/ within the layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs

RUN cargo build --release

# ── Full build ───────────────────────────────────────────────────────────────
COPY . .
RUN touch src/main.rs src/lib.rs

ARG GITHUB_SHA

RUN cargo build --release && \
    cp /app/target/release/xs3lerator /xs3lerator

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM ubuntu:24.04

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates adduser \
    && case "$TARGETARCH" in \
         amd64) FDB_ARCH=amd64 ;; \
         arm64) FDB_ARCH=aarch64 ;; \
       esac \
    && curl -fsSL "https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_${FDB_ARCH}.deb" -o /tmp/fdb-clients.deb \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /xs3lerator /usr/local/bin/xs3lerator

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/xs3lerator"]
