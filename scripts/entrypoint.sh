#!/bin/sh
set -e

# Defaults
PORT=${PULSE_PORT:-5555}
GRPC_PORT=${PULSE_GRPC_PORT:-5556}
DATA_DIR=${PULSE_DATA_DIR:-"/data"}

# Start Broker in background
echo "Starting Pulse Broker on HTTP:$PORT, gRPC:$GRPC_PORT..."
/app/pulse run \
  -port "$PORT" \
  -grpc-port "$GRPC_PORT" \
  -data-dir "$DATA_DIR" \
  &
BROKER_PID=$!

# Start UI if port is provided
if [ -n "$PULSE_UI_PORT" ]; then
    echo "Starting Pulse UI on port $PULSE_UI_PORT..."
    # The UI connects to the broker on localhost
    /app/pulse-ui \
      -pulse-url "http://localhost:$PORT" \
      -port "$PULSE_UI_PORT" \
      &
    UI_PID=$!
fi

# Wait for the broker process. 
# If the broker dies, the container should exit.
wait $BROKER_PID
