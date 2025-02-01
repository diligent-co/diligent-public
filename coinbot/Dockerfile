FROM golang:1.23.0-alpine AS builder

ENV GOCACHE=/root/.cache/go-build
ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /build

# Install system packages
RUN --mount=type=cache,target=/var/cache/apk \
    apk update && \
    apk add --no-cache tidyhtml && \
    tidy -v

# Download dependencies
RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
    go mod download -x

ARG COMMIT="none"
ARG VERSION="none"

# Test OpenGL availability
RUN apk add --no-cache \
    mesa-dev \
    xorg-server-dev \
    mesa-gl \
    mesa-gles \
    gcc \
    musl-dev \
    glxinfo && \
    glxinfo | grep "OpenGL version" || true

# Build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,source=.,target=. \
    go build -o /coinbot -ldflags \
        "-X 'coinbot/src/version.commit=${COMMIT}' \
        -X 'coinbot/src/version.version=${VERSION}' \
        -X 'coinbot/src/version.buildTimestamp=$(date -u +%Y-%m-%dT%H:%M:%S%Z)'" ./src

# Minimal containter
FROM alpine:latest

WORKDIR /app

COPY --from=arigaio/atlas:latest-alpine /atlas /bin/atlas

COPY --from=builder /coinbot .

ARG SERVER_PORT=8080
EXPOSE ${SERVER_PORT}
ENV PORT=${SERVER_PORT}

CMD ["./coinbot"]