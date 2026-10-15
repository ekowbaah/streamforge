FROM rust:1.90-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 streamforge
USER streamforge
WORKDIR /home/streamforge
COPY --from=build --chown=streamforge:streamforge /app/target/release/streamforge /usr/local/bin/streamforge
ENV STREAMFORGE_ADDR=0.0.0.0:8080 STREAMFORGE_DATA=/home/streamforge/data STREAMFORGE_PARTITIONS=4
EXPOSE 8080
CMD ["streamforge"]
