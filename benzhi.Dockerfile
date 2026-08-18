# syntax=docker/dockerfile:1.7
FROM golang:1.25.6
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
ENV GOTOOLCHAIN=local
WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,id=go-mod-1.25.6,target=/cache/go-mod \
    GOMODCACHE=/cache/go-mod go mod download \
    && mkdir -p /go/pkg/mod \
    && cp -a /cache/go-mod/. /go/pkg/mod/
COPY . .
RUN --mount=type=cache,id=go-build-1.25.6,target=/root/.cache/go-build go build ./...
CMD ["bash"]
