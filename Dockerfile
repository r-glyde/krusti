FROM rust:1.38 AS builder

USER root

WORKDIR /usr/src/krusti

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN apt-get update && apt-get install -y musl-dev musl-tools libssl-dev

RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --release --target=x86_64-unknown-linux-musl
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically-linked binary into a new container.
FROM alpine
COPY --from=builder /usr/local/cargo/bin/krusti .
USER root
CMD ["./krusti"]
