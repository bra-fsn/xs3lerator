# syntax=docker/dockerfile:1
#
# Multi-stage build producing a minimal static binary image.
# Targets musl so the result is fully statically linked — no libc dependency.
#
# Caching strategy: dependencies are built in a separate layer from application
# code. In CI (GHA), type=gha,mode=max caches ALL intermediate layers across
# runs, so the dependency layer is only rebuilt when Cargo.toml/Cargo.lock change.
# --mount=type=cache is intentionally NOT used — those caches live inside the
# ephemeral BuildKit builder and cannot be exported/persisted by any Docker
# cache backend.

# ── Build stage ──────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools \
        gcc-aarch64-linux-gnu \
        libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# Tell cargo which linker to use for each musl target
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_musl=aarch64-linux-gnu-gcc

WORKDIR /app

ARG TARGETARCH

# ── Dependency cache layer ───────────────────────────────────────────────────
# Copy only manifests + stub source so this layer is invalidated only when
# dependencies change. The compiled deps stay in /app/target/ within the layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs

RUN case "$TARGETARCH" in \
      amd64) TARGET=x86_64-unknown-linux-musl ;; \
      arm64) TARGET=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac && \
    cargo build --release --target "$TARGET"

# ── Full build ───────────────────────────────────────────────────────────────
COPY . .
RUN touch src/main.rs src/lib.rs

ARG GITHUB_SHA

RUN case "$TARGETARCH" in \
      amd64) TARGET=x86_64-unknown-linux-musl ;; \
      arm64) TARGET=aarch64-unknown-linux-musl ;; \
    esac && \
    cargo build --release --target "$TARGET" && \
    cp /app/target/"$TARGET"/release/xs3lerator /xs3lerator

# ── Runtime stage ────────────────────────────────────────────────────────────
# Ubuntu base so the image has a shell for debugging (e.g. docker run -it --entrypoint /bin/bash ...).
# Binary is musl-linked for the application code; libfdb_c.so is loaded at
# runtime by the foundationdb crate.
FROM ubuntu:24.04

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && case "$TARGETARCH" in \
         amd64) FDB_ARCH=amd64 ;; \
         arm64) FDB_ARCH=arm64 ;; \
       esac \
    && curl -fsSL "https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_${FDB_ARCH}.deb" -o /tmp/fdb-clients.deb \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /xs3lerator /usr/local/bin/xs3lerator

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/xs3lerator"]
