# pulse

Pulse is a small, honest message broker for learning and lightweight projects. This README explains how to build.

**Install / Uninstall (scripts)**
- **Install (system):** [scripts/install.sh](scripts/install.sh) installs the CLI/system components (if provided on your platform).
- **Uninstall:** [scripts/uninstall.sh](scripts/uninstall.sh) removes installed components.

**CLI reference**
Use the `pulse` CLI to control the broker and inspect topics. Examples assume the CLI binary is in your PATH.

- `pulse start [--port <http-port>] [--grpc-port <grpc-port>]`
	- Start the broker in foreground/daemon mode. Defaults: `--port 5555`, `--grpc-port 5556`.

- `pulse stop`
	- Stop a running broker (uses the CLI's management mechanism).

- `pulse status`
	- Show broker status (running, ports, PID if available).

- `pulse topics`
	- List all topic names.

- `pulse topic <name> [-m <offset>]`
	- Show stats for `<name>`. Use `-m <offset>` to fetch a single message at that offset (payload returned base64).

- `pulse publish --topic <name> [--body-file <file>]`
	- Publish raw bytes to `<name>`. If `--body-file` omitted, reads from STDIN.

- `pulse consume --topic <name> --consumer <id> [--max <n>]`
	- Consume messages for consumer `<id>`. `--max` limits number of messages (default configured in broker).

- `pulse commit --topic <name> --consumer <id> --offset <n>`
	- Commit offset `<n>` for consumer `<id>` on `<name>` (use the offset returned by consume).

- `pulse proto --export [--out <file>]`
	- Export the proto file used by the broker (convenience for clients). By default prints to STDOUT; use `--out` to save.

Notes:
- Many CLI commands call the broker's HTTP admin API under the hood. If the broker is not running, CLI commands that query it will fail.
- Use `pulse --help` for a quick summary of flags supported by your built binary.

**Python SDK (sdk/python)**
- Install into a venv for development: from `pip install pulse-broker`.
More details of sdk: [README.md](sdk/python/README.md)
