import grpc
import threading
import time
import contextvars
import json
from .config import get_config, get_topic_config
from .proto import pulse_pb2, pulse_pb2_grpc

# Registry of registered consumers
_consumers = []

# Context variable to hold the current message context for manual commit
_current_context = contextvars.ContextVar("current_context", default=None)

class MessageContext:
    def __init__(self, stub, topic, consumer_group, offset):
        self.stub = stub
        self.topic = topic
        self.consumer_group = consumer_group
        self.offset = offset
        self.committed = False

def commit():
    """
    Manually commit the current message offset.
    Must be called within a consumer handler.
    """
    ctx = _current_context.get()
    if not ctx:
        raise RuntimeError("commit() called outside of a consumer handler")
    
    if ctx.committed:
        return

    try:
        ctx.stub.CommitOffset(pulse_pb2.CommitOffsetRequest(
            topic=ctx.topic,
            consumer_name=ctx.consumer_group,
            offset=ctx.offset + 1
        ))
        ctx.committed = True
    except grpc.RpcError as e:
        print(f"Error committing offset: {e}")
        raise e

class Message:
    def __init__(self, proto_msg):
        self.offset = proto_msg.offset
        self.timestamp = proto_msg.timestamp
        self._raw_payload = proto_msg.payload
        # Headers from the broker (map<string,string>)
        # Some older messages may not have headers.
        try:
            self._headers = dict(proto_msg.headers)
        except Exception:
            self._headers = {}
    
    @property
    def payload(self):
        """Return payload converted to original type using headers.

        header `payload-type` values: 'json', 'string', 'bytes'.
        If missing, attempt to parse JSON and fall back to bytes.
        """
        ptype = self._headers.get("payload-type")
        if ptype == "json":
            try:
                return json.loads(self._raw_payload)
            except Exception:
                return self._raw_payload
        if ptype == "string":
            try:
                return self._raw_payload.decode("utf-8")
            except Exception:
                return self._raw_payload
        if ptype == "bytes":
            return self._raw_payload

        # Fallback for older messages: try JSON then bytes
        try:
            return json.loads(self._raw_payload)
        except Exception:
            return self._raw_payload
    
    @property
    def raw_payload(self):
        return self._raw_payload

    @property
    def headers(self):
        return self._headers

    def __str__(self):
        return f"Message(offset={self.offset}, payload={self.payload})"

def consumer(topic, host=None, port=None, consumer_group=None, auto_commit=None, grouped=True):
    """
    Decorator to register a function as a consumer for a topic.

    Parameters:
    - topic: topic name
    - consumer_group: explicit group id (overrides config client.id)
    - grouped (bool): when True (default), handlers with the same topic+group
      will share a single stream/consumer and messages will be distributed
      among them. When False, the decorator will create a unique consumer
      group id so the handler receives all messages independently.
    """
    def decorator(func):
        config = get_config()
        topic_config = get_topic_config(topic)

        # Determine configuration
        c_host = host or config["broker"]["host"]
        c_port = port or config["broker"]["grpc_port"]

        # Determine base consumer group
        base_group = consumer_group or config["client"]["id"]

        # If grouped is False, create a unique consumer id so it will consume
        # independently of other consumers using the same app id.
        if not grouped:
            import uuid
            c_group = f"{base_group}-{uuid.uuid4().hex}"
        else:
            c_group = base_group

        # Determine auto_commit
        c_auto_commit = auto_commit
        if c_auto_commit is None:
            c_auto_commit = config["client"]["auto_commit"]
            if topic_config and "consume" in topic_config:
                if "auto_commit" in topic_config["consume"]:
                    c_auto_commit = topic_config["consume"]["auto_commit"]

        # Register handler grouped by (topic, group). We'll keep a single
        # _consumers entry per (topic, group) and collect handlers so a single
        # stream can dispatch to them (avoiding duplicate delivery within the
        # same process when using grouped=True).
        _consumers.append({
            "topic": topic,
            "host": c_host,
            "port": c_port,
            "group": c_group,
            "auto_commit": c_auto_commit,
            "handler": func,
            "grouped": grouped
        })
        return func
    return decorator

def run():
    """
    Start all registered consumers.
    This function blocks.
    """
    # Coalesce consumers by (topic, group) when grouped=True so we create
    # a single streaming connection per group and dispatch messages to the
    # registered handlers in this process. If grouped=False was used, each
    # entry will already have a unique group id and will be treated separately.
    grouped_map = {}
    for c in _consumers:
        key = (c["topic"], c["host"], c["port"], c["group"])
        if key not in grouped_map:
            grouped_map[key] = {
                "topic": c["topic"],
                "host": c["host"],
                "port": c["port"],
                "group": c["group"],
                "auto_commit": c["auto_commit"],
                "handlers": []
            }
        grouped_map[key]["handlers"].append(c["handler"])

    threads = []
    for key, gc in grouped_map.items():
        t = threading.Thread(target=_consume_loop_group, args=(gc,), daemon=True)
        t.start()
        threads.append(t)
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping consumers...")

def _consume_loop(c_config):
    address = f"{c_config['host']}:{c_config['port']}"
    channel = grpc.insecure_channel(address)
    stub = pulse_pb2_grpc.PulseServiceStub(channel)
    
    # Backwards-compatible single-consumer loop (kept for compatibility with
    # any direct calls). Wrap into group-style data and forward to group loop.
    gc = {
        "topic": c_config["topic"],
        "host": c_config["host"],
        "port": c_config["port"],
        "group": c_config["group"],
        "auto_commit": c_config["auto_commit"],
        "handlers": [c_config.get("handler")]
    }
    _consume_loop_group(gc)


def _consume_loop_group(group_config):
    topic = group_config["topic"]
    group = group_config["group"]
    auto_commit = group_config["auto_commit"]
    handlers = group_config.get("handlers", [])

    address = f"{group_config['host']}:{group_config['port']}"
    channel = grpc.insecure_channel(address)
    stub = pulse_pb2_grpc.PulseServiceStub(channel)

    print(f"Starting consumer for topic '{topic}' (group: {group}) on {address}")

    # simple round-robin index for dispatching to handlers
    handler_idx = 0

    while True:
        try:
            request = pulse_pb2.ConsumeRequest(
                topic=topic,
                consumer_name=group,
                offset=0
            )
            stream = stub.Consume(request)

            for proto_msg in stream:
                msg = Message(proto_msg)

                # Round-robin select handler
                if not handlers:
                    # No handlers registered; skip
                    continue
                handler = handlers[handler_idx % len(handlers)]
                handler_idx += 1

                # Set context for manual commit
                ctx = MessageContext(stub, topic, group, msg.offset)
                token = _current_context.set(ctx)

                try:
                    handler(msg)

                    # Auto-commit if enabled and not manually committed
                    if auto_commit and not ctx.committed:
                        commit()

                except Exception as e:
                    print(f"Error processing message: {e}")
                finally:
                    _current_context.reset(token)

        except grpc.RpcError as e:
            print(f"Connection lost for {topic}: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error in consumer {topic}: {e}")
            time.sleep(5)
