# syntax=docker/dockerfile:1
# Builds ONLY the collector. Build context is the repo root (the root Makefile
# passes "." as the context) because the collector uses the broker's client library
# through `replace broker => ../broker`.
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src/collector
COPY broker/go.mod broker/go.sum /src/broker/
COPY collector/go.mod collector/go.sum ./
RUN go mod download
COPY broker/pkg /src/broker/pkg
COPY broker/internal /src/broker/internal
COPY collector/cmd ./cmd
COPY collector/internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
      -ldflags="-s -w -X main.version=${VERSION}" -o /out/collector ./cmd/collector

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/collector /collector
USER 65532:65532
EXPOSE 9090
ENTRYPOINT ["/collector"]
