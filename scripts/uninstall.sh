#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)     os=Linux;;
        Darwin*)    os=Mac;;
        CYGWIN*)    os=Windows;;
        MINGW*)     os=Windows;;
        MSYS*)      os=Windows;;
        *)          os="UNKNOWN:${uname -s}";;
    esac
    echo $os
}

OS=$(detect_os)
BINARY_NAME="pulse"
if [ "$OS" = "Windows" ]; then
    BINARY_NAME="pulse.exe"
fi

INSTALL_DIR="/usr/local/bin"
BINARY_PATH="$INSTALL_DIR/$BINARY_NAME"

echo "Uninstalling Pulse..."

# 1. Stop the service if running
echo "Stopping Pulse service..."

# Try to stop using PID file first
TMP_DIR="${TMPDIR:-/tmp}"
TMP_DIR=${TMP_DIR%/}
PID_FILE="$TMP_DIR/pulse.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null; then
        echo "Killing process $PID..."
        kill "$PID" || true
    else
        echo "Process $PID not found."
    fi
    rm "$PID_FILE" || true
else
    # Fallback: try to stop using the command, or pkill
    if command -v pulse &> /dev/null; then
        pulse stop || true
    fi
    # Last resort: pkill
    pkill -x "pulse" || true
fi

# 2. Remove the binary
if [ -f "$BINARY_PATH" ]; then
    echo "Removing binary from $BINARY_PATH..."
    if [ -w "$INSTALL_DIR" ]; then
        rm "$BINARY_PATH"
    else
        echo "Sudo permissions required to remove from $INSTALL_DIR"
        sudo rm "$BINARY_PATH"
    fi
    echo "Binary removed."
else
    echo "Binary not found at $BINARY_PATH. Skipping."
fi

# 3. Clean up temporary files (PID and Logs)
# Go's os.TempDir() usually maps to $TMPDIR on macOS/Linux or /tmp
TMP_DIR="${TMPDIR:-/tmp}"
# Remove trailing slash if present
TMP_DIR=${TMP_DIR%/}

PID_FILE="$TMP_DIR/pulse.pid"
LOG_FILE="$TMP_DIR/pulse.log"

if [ -f "$PID_FILE" ]; then
    echo "Removing PID file..."
    rm "$PID_FILE" || true
fi

if [ -f "$LOG_FILE" ]; then
    echo "Removing log file ($LOG_FILE)..."
    rm "$LOG_FILE" || true
fi

echo "--------------------------------------------------"
echo "Pulse uninstalled successfully."
echo "--------------------------------------------------"
