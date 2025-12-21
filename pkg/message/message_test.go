package message

import (
	"bytes"
	"testing"
)

func TestMessage_SerializeDeserialize(t *testing.T) {
	payload := []byte("test-payload")
	msg := NewMessage(123, payload)

	// Serialize
	data, err := msg.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	msg2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if msg2.Offset != msg.Offset {
		t.Errorf("Offset mismatch. Want %d, got %d", msg.Offset, msg2.Offset)
	}
	if !bytes.Equal(msg2.Payload, msg.Payload) {
		t.Errorf("Payload mismatch")
	}
	// Timestamp might differ slightly due to serialization precision if not handled, 
	// but msgpack usually handles int64 fine.
	if msg2.Timestamp != msg.Timestamp {
		t.Errorf("Timestamp mismatch")
	}
}
