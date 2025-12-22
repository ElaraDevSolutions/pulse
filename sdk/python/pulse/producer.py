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

    def send(self, topic, payload):
        """
        Send a message to a topic.
        payload can be bytes or a dict (which will be JSON serialized).
        """
        if isinstance(payload, dict):
            data = json.dumps(payload).encode("utf-8")
        elif isinstance(payload, str):
            data = payload.encode("utf-8")
        elif isinstance(payload, bytes):
            data = payload
        else:
            raise ValueError("Payload must be bytes, str, or dict")

        request = pulse_pb2.PublishRequest(
            topic=topic,
            payload=data
        )
        
        try:
            self.stub.Publish(request)
        except grpc.RpcError as e:
            # TODO: Handle retries based on config
            raise e

    def close(self):
        self.channel.close()
