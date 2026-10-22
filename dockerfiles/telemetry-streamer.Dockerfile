# syntax=docker/dockerfile:1
# Builds ONLY the streamer. Build context is telemetry-streamer/ (see `make docker`).
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/streamer ./cmd/streamer
COPY internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
      -ldflags="-s -w -X main.version=${VERSION}" -o /out/streamer ./cmd/streamer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/streamer /streamer
# The telemetry CSV is baked into the image: changing it means rebuilding the image.
COPY --from=build /src/internal/resource/dcgm.csv /data/telemetry.csv
ENV STREAMER_CSV_PATH=/data/telemetry.csv
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["/streamer"]
