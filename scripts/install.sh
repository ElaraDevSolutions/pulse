#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Ensure we are running from the project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

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
echo "Detected OS: $OS"

# Define binary name based on OS
BINARY_NAME="pulse"
if [ "$OS" = "Windows" ]; then
    BINARY_NAME="pulse.exe"
fi

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed or not in PATH."
    exit 1
fi

# Build the static binary
echo "Building Pulse static binary ($BINARY_NAME)..."
# CGO_ENABLED=0 ensures a static binary (no dynamic linking to C libraries)
# -ldflags="-s -w" strips debug info to reduce size
export CGO_ENABLED=0
if go build -ldflags="-s -w" -o "$BINARY_NAME" cmd/broker/main.go; then
    echo "Build successful."
else
    echo "Build failed."
    exit 1
fi

# Define installation directory based on OS/Environment
# /usr/local/bin is standard for user binaries on Linux and macOS
INSTALL_DIR="/usr/local/bin"

echo "Installing Pulse to $INSTALL_DIR..."

# Check if the directory exists
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Directory $INSTALL_DIR does not exist. Attempting to create..."
    if [ -w "$(dirname "$INSTALL_DIR")" ]; then
        mkdir -p "$INSTALL_DIR"
    else
        sudo mkdir -p "$INSTALL_DIR"
    fi
fi

# Move binary to install directory
if [ -w "$INSTALL_DIR" ]; then
    cp "$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    chmod +x "$INSTALL_DIR/$BINARY_NAME"
    rm "$BINARY_NAME"
else
    echo "Sudo permissions required to write to $INSTALL_DIR"
    sudo cp "$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"
    rm "$BINARY_NAME"
fi

# Verify installation
# On Windows/Git Bash, 'command -v pulse' might find pulse.exe even if we ask for pulse
if command -v pulse &> /dev/null || command -v pulse.exe &> /dev/null; then
    echo "--------------------------------------------------"
    echo "Pulse installed successfully!"
    echo "Location: $(which pulse 2>/dev/null || which pulse.exe)"
    echo "You can now run 'pulse --port 5555' from any directory."
    echo "--------------------------------------------------"
else
    echo "Warning: Installation completed, but 'pulse' was not found in your PATH."
    echo "Please ensure $INSTALL_DIR is in your PATH."
fi
