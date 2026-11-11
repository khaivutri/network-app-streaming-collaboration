# ==============================================================================
# Stage 1: Build Stage (Compile Server & Client Binaries)
# ==============================================================================
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copy module definition first for layer caching
COPY go.mod ./

# Copy source code (including embedded web UI assets)
COPY . .

# Build static binaries without CGO and strip debug symbols for minimal image size
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

# ==============================================================================
# Stage 2: Runtime Stage (Minimal Alpine Image)
# ==============================================================================
FROM alpine:3.20 AS runtime

# Install iproute2 (provides `tc` / netem for network simulation if needed)
RUN apk add --no-cache ca-certificates iproute2

WORKDIR /app

# Copy compiled binaries into system PATH
COPY --from=builder /out/server /usr/local/bin/server
COPY --from=builder /out/client /usr/local/bin/client

# Copy default user credentials configuration
COPY configs/users.json /app/configs/users.json

# Create default storage directories for client staging and downloads
RUN mkdir -p /app/storage/staging /app/storage/downloads

# Expose UDP port for Central RUDP Server and TCP port for Local Client Web UI
EXPOSE 9000/udp
EXPOSE 8081/tcp

# Default command starts the Central RUDP Server (override in docker-compose for clients)
CMD ["server", "--mode", "full", "--addr", "0.0.0.0:9000", "--users", "configs/users.json"]

