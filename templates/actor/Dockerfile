# Builds the Actor into a small image.
FROM rust:1-slim-trixie AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# We use `debian:trixie-slim` because it is the base of the Apify Python and Node.js images, so it is often cached on the platform.
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/crawlee-rs-actor /usr/local/bin/actor
# The Actor looks for `.actor/input_schema.json` in its working directory, when run locally.
COPY .actor .actor
CMD ["actor"]
