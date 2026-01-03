# Pulse Docker Image

Pulse is a lightweight, honest message broker designed for learning and small projects. This Docker image allows you to run Pulse easily with optional UI support.

## Quick Start

Run the broker with default settings:

```bash
docker run -d -p 5555:5555 -p 5556:5556 <username>/pulse:latest
```

## Configuration

The image is configured using environment variables.

### Core Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PULSE_PORT` | The HTTP port for the broker API. | `5555` | No |
| `PULSE_GRPC_PORT` | The gRPC port for the broker. | `5556` | No |
| `PULSE_DATA_DIR` | Directory inside the container to store data. | `/data` | No |

### UI Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PULSE_UI_PORT` | If set, starts the Web UI on this port. | - | No |

## Ports

- **5555** (TCP): Default HTTP API port.
- **5556** (TCP): Default gRPC port.
- **<PULSE_UI_PORT>** (TCP): The port you chose for the UI (if enabled).

## Persistence

To persist data (topics, messages) across restarts, mount a volume to the data directory (default `/data`).

```bash
docker run -d \
  -v pulse-data:/data \
  -p 5555:5555 \
  -p 5556:5556 \
  <username>/pulse:latest
```

## Examples

### 1. Run with Web UI

Start the broker and enable the UI on port 8080:

```bash
docker run -d \
  -e PULSE_UI_PORT=8080 \
  -p 5555:5555 \
  -p 5556:5556 \
  -p 8080:8080 \
  <username>/pulse:latest
```

Access the UI at `http://localhost:8080`.

### 2. Custom Ports

Run the broker on HTTP port 6000 and gRPC port 6001:

```bash
docker run -d \
  -e PULSE_PORT=6000 \
  -e PULSE_GRPC_PORT=6001 \
  -p 6000:6000 \
  -p 6001:6001 \
  <username>/pulse:latest
```
