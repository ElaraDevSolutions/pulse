package pulse

import (
	pb "pulse/pkg/proto"
)

type Message struct {
	Payload   []byte
	Offset    int64
	Timestamp int64
	Headers   map[string]string
}

func newMessage(p *pb.ConsumeResponse) *Message {
	return &Message{
		Payload:   p.Payload,
		Offset:    int64(p.Offset),
		Timestamp: p.Timestamp,
		Headers:   p.Headers,
	}
}
