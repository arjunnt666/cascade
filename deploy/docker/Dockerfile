FROM rust:1-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p cascade-cli

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/cascade /usr/local/bin/cascade
ENV CASCADE_ADDR=0.0.0.0:7233 CASCADE_DATA=/data
VOLUME /data
EXPOSE 7233
CMD ["cascade", "serve"]
