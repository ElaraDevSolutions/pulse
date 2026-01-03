import pytest
from unittest.mock import MagicMock, patch, ANY
import pulse
from pulse.proto import pulse_pb2

# --- Producer Tests ---

def test_producer_init():
    with patch('pulse.producer.grpc.insecure_channel') as mock_channel:
        p = pulse.Producer(host="myhost", port=9999)
        mock_channel.assert_called_with("myhost:9999")

def test_producer_send_dict():
    with patch('pulse.producer.grpc.insecure_channel'):
        mock_stub = MagicMock()
        with patch('pulse.producer.pulse_pb2_grpc.PulseServiceStub', return_value=mock_stub):
            p = pulse.Producer()
            p.send("topic1", {"key": "value"})
            
            mock_stub.Publish.assert_called_once()
            args, _ = mock_stub.Publish.call_args
            req = args[0]
            assert req.topic == "topic1"
            assert req.payload == b'{"key": "value"}'

def test_producer_send_bytes():
    with patch('pulse.producer.grpc.insecure_channel'):
        mock_stub = MagicMock()
        with patch('pulse.producer.pulse_pb2_grpc.PulseServiceStub', return_value=mock_stub):
            p = pulse.Producer()
            p.send("topic1", b"raw-data")
            
            args, _ = mock_stub.Publish.call_args
            req = args[0]
            assert req.payload == b"raw-data"

def test_producer_stream_send():
    with patch('pulse.producer.grpc.insecure_channel'):
        mock_stub = MagicMock()
        with patch('pulse.producer.pulse_pb2_grpc.PulseServiceStub', return_value=mock_stub):
            p = pulse.Producer()
            
            # Mock the return value of StreamPublish
            mock_summary = pulse_pb2.PublishSummary(
                succeeded_count=2,
                failed_count=0,
                last_error=""
            )
            mock_stub.StreamPublish.return_value = mock_summary
            
            messages = [("topic1", b"msg1"), ("topic1", b"msg2")]
            result = p.stream_send(messages)
            
            assert result == mock_summary
            mock_stub.StreamPublish.assert_called_once()

# --- Consumer Tests ---

def test_consumer_decorator_registration():
    # Clear consumers
    from pulse.consumer import _consumers
    _consumers.clear()
    
    @pulse.consumer("test-topic", consumer_group="my-group")
    def my_handler(msg):
        pass
    
    assert len(_consumers) == 1
    c = _consumers[0]
    assert c["topic"] == "test-topic"
    assert c["group"] == "my-group"
    assert c["handler"] == my_handler

def test_commit_outside_context():
    with pytest.raises(RuntimeError):
        pulse.commit()

# --- Config Tests ---

def test_load_config_defaults():
    cfg = pulse.load_config(path="nonexistent")
    assert cfg["broker"]["http_port"] == 5555

def test_load_config_file(tmp_path):
    f = tmp_path / "pulse.yaml"
    f.write_text("""
broker:
  http_port: 8080
client:
  id: "test-client"
""")
    cfg = pulse.load_config(path=str(f))
    assert cfg["broker"]["http_port"] == 8080
    assert cfg["client"]["id"] == "test-client"
    # Check default preserved
    assert cfg["broker"]["grpc_port"] == 5556
