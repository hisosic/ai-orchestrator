# AI Container Orchestrator (Go) - multi-stage build (linux/amd64)
FROM --platform=linux/amd64 golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /orchestrator ./cmd/orchestrator

# Docker CLI from official image
FROM --platform=linux/amd64 docker:27-cli AS docker-cli

# Runtime
FROM --platform=linux/amd64 debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates bash curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

WORKDIR /app
COPY --from=builder /orchestrator /app/orchestrator
COPY static /app/static
COPY services /app/services

ENV ORCHESTRATOR_STATE_DIR=/data
RUN mkdir -p /data

EXPOSE 8000

CMD ["/app/orchestrator"]
