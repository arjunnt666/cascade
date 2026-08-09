# multi-stage build for cascade server / cli
FROM rust:1.80-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p cascade-cli -p cascade-server || true

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/cascade /usr/local/bin/cascade || true
EXPOSE 7233
CMD ["cascade", "version"]
