# syntax=docker/dockerfile:1

ARG DOCKER_HUB="docker.io"

FROM ${DOCKER_HUB}/golang:1.25-alpine AS go-builder

RUN apk add --no-cache build-base git

WORKDIR /build/matrix-inline
ENV GOPATH=/go \
    GOMODCACHE=/go/pkg/mod \
    GOCACHE=/root/.cache/go-build

COPY matrix-inline/go.mod matrix-inline/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY matrix-inline/cmd ./cmd
COPY matrix-inline/pkg ./pkg
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -tags goolm -o /out/matrix-inline ./cmd/matrix-inline

FROM ${DOCKER_HUB}/rust:1.96-alpine AS rust-builder

RUN apk add --no-cache build-base protobuf-dev sqlite-dev sqlite-static

WORKDIR /build
ENV CARGO_HOME=/cargo \
    CARGO_TARGET_DIR=/target

COPY inline-public/Cargo.toml inline-public/Cargo.lock inline-public/rust-toolchain.toml ./inline-public/
COPY inline-public/cli ./inline-public/cli
COPY inline-public/crates ./inline-public/crates
COPY matrix-inline/Cargo.toml matrix-inline/Cargo.lock matrix-inline/rust-toolchain.toml ./matrix-inline/
COPY matrix-inline/crates ./matrix-inline/crates
WORKDIR /build/matrix-inline
RUN --mount=type=cache,target=/cargo/registry \
    --mount=type=cache,target=/cargo/git \
    --mount=type=cache,target=/target \
    cargo build --release -p matrix-inline-adapter && \
    cp /target/release/matrix-inline-adapter /out-matrix-inline-adapter

FROM ${DOCKER_HUB}/alpine:3.23

ENV PUID=1337 \
    PGID=1337 \
    BRIDGEV2=1 \
    RUST_LOG=info \
    INLINE_SIDECAR_BIND=127.0.0.1:29342

RUN apk add --no-cache bash ca-certificates curl jq sqlite-libs su-exec yq-go

COPY --from=go-builder /out/matrix-inline /usr/bin/matrix-inline
COPY --from=rust-builder /out-matrix-inline-adapter /usr/bin/matrix-inline-adapter
COPY matrix-inline/docker-run.sh /docker-run.sh
RUN chmod +x /docker-run.sh

WORKDIR /data
VOLUME ["/data"]
EXPOSE 29343

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:29342/health >/dev/null || exit 1

CMD ["/docker-run.sh"]
