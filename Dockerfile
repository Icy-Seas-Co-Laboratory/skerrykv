FROM rust:1.94-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin skerry

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && groupadd --system skerry && useradd --system --gid skerry --home-dir /var/lib/skerry skerry \
 && mkdir -p /var/lib/skerry/data && chown -R skerry:skerry /var/lib/skerry
RUN printf '[server]\nlisten = "0.0.0.0:7788"\n\n[storage]\nroot = "/var/lib/skerry/data"\n' > /var/lib/skerry/config.toml
COPY --from=build /src/target/release/skerry /usr/local/bin/skerry
USER skerry
WORKDIR /var/lib/skerry
ENV SKERRY_MCP_ENABLED=false
ENV SKERRY_CONFIG=/var/lib/skerry/config.toml
EXPOSE 7788
VOLUME ["/var/lib/skerry"]
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 CMD ["skerry", "health", "--url", "http://127.0.0.1:7788"]
ENTRYPOINT ["/usr/local/bin/skerry"]
CMD ["serve", "--config", "/var/lib/skerry/config.toml"]
