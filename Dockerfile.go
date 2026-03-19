# Build stage
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /orchestrator ./cmd/orchestrator

# Runtime stage
FROM alpine:3.19
RUN apk add --no-cache ca-certificates docker-cli bash python3 openssh-client sshpass
COPY --from=builder /orchestrator /usr/local/bin/orchestrator
COPY static/ /app/static/
COPY services/ /app/services/
WORKDIR /app
ENV ORCHESTRATOR_STATE_DIR=/data
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --retries=3 CMD wget -q --spider http://localhost:8000/health || exit 1
ENTRYPOINT ["orchestrator"]
