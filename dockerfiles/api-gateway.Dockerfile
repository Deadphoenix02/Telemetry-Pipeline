# syntax=docker/dockerfile:1
# Builds ONLY the api-gateway. Build context is api-gateway/ (see `make docker`).
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY openapi ./openapi
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
      -ldflags="-s -w -X main.version=${VERSION}" -o /out/api-gateway ./cmd/api-gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api-gateway /api-gateway
USER 65532:65532
# 8080 the API, 9090 admin (/healthz /readyz /metrics)
EXPOSE 8080 9090
ENTRYPOINT ["/api-gateway"]
