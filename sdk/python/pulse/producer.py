import json
import grpc
from .config import get_config
from .proto import pulse_pb2, pulse_pb2_grpc

class Producer:
    def __init__(self, host=None, port=None):
        config = get_config()
        self.host = host or config["broker"]["host"]
        self.port = port or config["broker"]["grpc_port"]
        self.address = f"{self.host}:{self.port}"
        
        self.channel = grpc.insecure_channel(self.address)
        self.stub = pulse_pb2_grpc.PulseServiceStub(self.channel)
        
        self._setup_topics(config)

    def _setup_topics(self, config):
        for topic_cfg in config.get("topics", []):
            if topic_cfg.get("create_if_missing", False):
                name = topic_cfg["name"]
                t_config = topic_cfg.get("config", {})
                fifo = t_config.get("fifo", False)
                retention_bytes = t_config.get("retention_bytes", 0)
                
                req = pulse_pb2.CreateTopicRequest(
                    topic=name,
                    fifo=fifo,
                    retention_bytes=retention_bytes
                )
                try:
                    self.stub.CreateTopic(req)
                except grpc.RpcError:
                    # Ignore errors (e.g. topic already exists)
                    pass

    def send(self, topic, payload):
        """
        Send a message to a topic.
        payload can be bytes or a dict (which will be JSON serialized).
        """
        headers = {}
        if isinstance(payload, dict):
            data = json.dumps(payload).encode("utf-8")
            headers["payload-type"] = "json"
        elif isinstance(payload, str):
            data = payload.encode("utf-8")
            headers["payload-type"] = "string"
        elif isinstance(payload, bytes):
            data = payload
            headers["payload-type"] = "bytes"
        else:
            raise ValueError("Payload must be bytes, str, or dict")

        request = pulse_pb2.PublishRequest(
            topic=topic,
            payload=data,
            headers=headers
        )
        
        try:
            self.stub.Publish(request)
        except grpc.RpcError as e:
            # TODO: Handle retries based on config
            raise e

    def stream_send(self, message_iterator):
        """
        Send a stream of messages to the broker.
        message_iterator should yield (topic, payload) tuples.
        """
        def request_generator():
            for topic, payload in message_iterator:
                headers = {}
                if isinstance(payload, dict):
                    data = json.dumps(payload).encode("utf-8")
                    headers["payload-type"] = "json"
                elif isinstance(payload, str):
                    data = payload.encode("utf-8")
                    headers["payload-type"] = "string"
                elif isinstance(payload, bytes):
                    data = payload
                    headers["payload-type"] = "bytes"
                else:
                    raise ValueError("Payload must be bytes, str, or dict")

                yield pulse_pb2.PublishRequest(
                    topic=topic,
                    payload=data,
                    headers=headers
                )

        try:
            # Get the single summary response
            summary = self.stub.StreamPublish(request_generator())
            return summary
        except grpc.RpcError as e:
            raise e

    def close(self):
        self.channel.close()
