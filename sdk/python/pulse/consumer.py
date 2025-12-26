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

def consumer(topic, host=None, port=None, consumer_group=None, auto_commit=None):
    """
    Decorator to register a function as a consumer for a topic.
    """
    def decorator(func):
        config = get_config()
        topic_config = get_topic_config(topic)
        
        # Determine configuration
        c_host = host or config["broker"]["host"]
        c_port = port or config["broker"]["grpc_port"]
        
        # Determine consumer group
        c_group = consumer_group
        if not c_group:
            c_group = config["client"]["id"]
        
        # Determine auto_commit
        c_auto_commit = auto_commit
        if c_auto_commit is None:
            c_auto_commit = config["client"]["auto_commit"]
            if topic_config and "consume" in topic_config:
                if "auto_commit" in topic_config["consume"]:
                    c_auto_commit = topic_config["consume"]["auto_commit"]

        _consumers.append({
            "topic": topic,
            "host": c_host,
            "port": c_port,
            "group": c_group,
            "auto_commit": c_auto_commit,
            "handler": func
        })
        return func
    return decorator

def run():
    """
    Start all registered consumers.
    This function blocks.
    """
    threads = []
    for c in _consumers:
        t = threading.Thread(target=_consume_loop, args=(c,), daemon=True)
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
    
    topic = c_config["topic"]
    group = c_config["group"]
    auto_commit = c_config["auto_commit"]
    handler = c_config["handler"]

    print(f"Starting consumer for topic '{topic}' (group: {group}) on {address}")

    while True:
        try:
            # Start streaming
            request = pulse_pb2.ConsumeRequest(
                topic=topic,
                consumer_name=group,
                offset=0 # 0 means "continue from last committed" usually, or we need to handle it
            )
            
            stream = stub.Consume(request)
            
            for proto_msg in stream:
                msg = Message(proto_msg)
                
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
                    # TODO: Handle DLQ or retry
                finally:
                    _current_context.reset(token)
                    
        except grpc.RpcError as e:
            print(f"Connection lost for {topic}: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error in consumer {topic}: {e}")
            time.sleep(5)
