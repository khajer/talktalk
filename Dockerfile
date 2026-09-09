# syntax=docker/dockerfile:1

### Build stage ###
FROM rust:1.90-slim AS builder
WORKDIR /app

# Cache dependencies separately from source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
# Touch main.rs so cargo re-links against the real sources instead of the
# placeholder built above.
RUN touch src/main.rs && cargo build --release

### Runtime stage ###
FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 talktalk
USER talktalk
WORKDIR /home/talktalk

COPY --from=builder /app/target/release/talktalk /usr/local/bin/talktalk

ENV TALKTALK_HOST=0.0.0.0:8080
EXPOSE 8080

ENTRYPOINT ["talktalk"]
