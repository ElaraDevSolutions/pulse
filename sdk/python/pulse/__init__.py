# Try to update proto first
try:
    from .proto_manager import ensure_proto
    import os
    host = os.environ.get("PULSE_HOST", "localhost")
    port = int(os.environ.get("PULSE_HTTP_PORT", "5555"))
    ensure_proto(host, port)
except Exception:
    pass

from .producer import Producer
from .consumer import consumer, commit, run
from .config import load_config

__all__ = ["Producer", "consumer", "commit", "run", "load_config"]
