# syntax=docker/dockerfile:1
# Builds ONLY the broker. Build context is broker/ (see `make docker`).
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
      -ldflags="-s -w -X main.version=${VERSION}" -o /out/broker ./cmd/broker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/broker /broker
USER 65532:65532
# 7070 gRPC (Ingest, Broker, Coordinator), 8080 HTTP (/healthz /readyz /metrics /lag)
EXPOSE 7070 8080
ENTRYPOINT ["/broker"]
