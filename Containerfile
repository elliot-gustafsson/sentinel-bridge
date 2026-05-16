FROM docker.io/library/rust:1.89-slim-trixie AS builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

RUN rm -rf src
COPY src ./src

RUN touch src/main.rs

RUN cargo build --release

FROM gcr.io/distroless/cc-debian13

USER nonroot:nonroot

COPY --from=builder --chown=nonroot:nonroot /usr/src/app/target/release/sentinel-bridge /usr/local/bin/

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/sentinel-bridge"]
