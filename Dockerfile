# syntax=docker/dockerfile:1

# Build stage. modernc.org/sqlite is a pure-Go SQLite driver, so the binary is
# statically linked with CGO disabled and runs on both linux/amd64 and linux/arm64.
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/aliquotseal ./cmd/aliquotseal

# Runtime stage.
FROM --platform=$TARGETPLATFORM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/aliquotseal /aliquotseal
EXPOSE 8080
ENTRYPOINT ["/aliquotseal"]
