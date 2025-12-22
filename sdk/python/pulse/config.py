import os
import yaml

DEFAULT_CONFIG = {
    "broker": {
        "host": "localhost",
        "http_port": 5555,
        "grpc_port": 5556,
        "timeout_ms": 5000
    },
    "client": {
        "id": "python-client",
        "auto_commit": True,
        "max_retries": 3
    },
    "topics": []
}

def load_config(path=None):
    """
    Load configuration from a YAML file.
    If path is not provided, looks for pulse.yaml or pulse.yml in the current directory.
    """
    if not path:
        if os.path.exists("pulse.yaml"):
            path = "pulse.yaml"
        elif os.path.exists("pulse.yml"):
            path = "pulse.yml"
    
    config = DEFAULT_CONFIG.copy()
    
    if path and os.path.exists(path):
        with open(path, "r") as f:
            file_config = yaml.safe_load(f)
            if file_config:
                _merge_dicts(config, file_config)
    
    return config

def _merge_dicts(base, update):
    """Recursively merge dictionaries."""
    for k, v in update.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            _merge_dicts(base[k], v)
        else:
            base[k] = v

# Global config instance
_config = load_config()

def get_config():
    return _config

def get_topic_config(topic_name):
    """Get configuration for a specific topic."""
    for topic in _config.get("topics", []):
        if topic.get("name") == topic_name:
            return topic
    return None
