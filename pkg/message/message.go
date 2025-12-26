package message

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Message represents a single unit of data in the broker.
type Message struct {
	Offset    uint64            `msgpack:"offset"`
	Timestamp int64             `msgpack:"timestamp"`
	Payload   []byte            `msgpack:"payload"`
	Headers   map[string]string `msgpack:"headers"`
}

// NewMessage creates a new Message with the current timestamp.
func NewMessage(offset uint64, payload []byte, headers map[string]string) Message {
	return Message{
		Offset:    offset,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
		Headers:   headers,
	}
}

// Serialize converts the Message into a byte slice using MessagePack.
func (m *Message) Serialize() ([]byte, error) {
	return msgpack.Marshal(m)
}

// Deserialize converts a byte slice back into a Message using MessagePack.
func Deserialize(data []byte) (*Message, error) {
	var m Message
	if err := msgpack.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
