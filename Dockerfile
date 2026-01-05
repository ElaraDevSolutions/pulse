# Build Stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY . .
# Build Broker
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /bin/pulse ./cmd/broker
# Build UI
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /bin/pulse-ui ./cmd/ui

# Final Stage
FROM alpine:latest
WORKDIR /app

# Install ca-certificates
RUN apk --no-cache add ca-certificates

# Copy binaries
COPY --from=builder /bin/pulse /app/pulse
COPY --from=builder /bin/pulse-ui /app/pulse-ui

# Copy UI assets
COPY cmd/ui/templates /app/templates
COPY cmd/ui/static /app/static

# Copy entrypoint
COPY scripts/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Create data directory
RUN mkdir -p /data

# Expose ports (Documentation)
# 5555: HTTP, 5556: gRPC
EXPOSE 5555 5556

ENTRYPOINT ["/app/entrypoint.sh"]
