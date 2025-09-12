# Build stage
FROM golang:1.21-alpine AS builder

# Install git for go modules
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o fio-distributed-tool .

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/fio-distributed-tool .
COPY --from=builder /app/templates ./templates
COPY --from=builder /app/config-sample.yaml .

# Change ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Default command
ENTRYPOINT ["./fio-distributed-tool"]
CMD ["-help"]
