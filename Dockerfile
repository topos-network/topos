ARG TOOLCHAIN_VERSION
FROM --platform=${BUILDPLATFORM:-linux/amd64} ghcr.io/topos-protocol/rust_builder:bullseye-${TOOLCHAIN_VERSION} AS base

ARG FEATURES
# Rust cache
ARG SCCACHE_S3_KEY_PREFIX
ARG SCCACHE_BUCKET
ARG SCCACHE_REGION
ARG RUSTC_WRAPPER
ARG PROTOC_VERSION=22.2

WORKDIR /usr/src/app

FROM --platform=${BUILDPLATFORM:-linux/amd64} base AS build
COPY . .
RUN --mount=type=secret,id=aws,target=/root/.aws/credentials \
    --mount=type=cache,id=sccache,target=/root/.cache/sccache \
  cargo build --release --no-default-features --features=${FEATURES} \
  && sccache --show-stats

FROM --platform=${BUILDPLATFORM:-linux/amd64} debian:bullseye-slim AS topos

ENV TCE_PORT=9090
ENV USER=topos
ENV UID=10001
ENV PATH="${PATH}:/usr/src/app"

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

WORKDIR /usr/src/app

COPY --from=build /usr/src/app/target/release/topos .
COPY tools/init.sh ./init.sh
COPY tools/liveness.sh /tmp/liveness.sh

RUN apt-get update && apt-get install -y \
    ca-certificates \
    jq \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER topos:topos

RUN mkdir /tmp/shared
RUN mkdir -p /tmp/node_config/subnet/topos
RUN mkdir -p /tmp/node_config/node/test/consensus
RUN mkdir -p /tmp/node_config/node/test/libp2p

ENTRYPOINT ["./init.sh"]
