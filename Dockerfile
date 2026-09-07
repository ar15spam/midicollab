FROM rust:1.89-bookworm AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libasound2-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin studio_server


FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/studio_server /app/studio_server

CMD ["/app/studio_server"]