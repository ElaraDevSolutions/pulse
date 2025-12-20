package logstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"pulse/pkg/message"
)

const (
	// MaxSegmentSize defines the maximum size of a log segment before rotation (10MB).
	MaxSegmentSize = 10 * 1024 * 1024
)

// Segment represents a single log file.
type Segment struct {
	BaseOffset uint64
	FilePath   string
	File       *os.File
	Size       int64
}

// AppendOnlyLog manages the persistent log with rotating segments.
type AppendOnlyLog struct {
	Dir           string
	mu            sync.RWMutex
	ActiveSegment *Segment
	Segments      []*Segment
	GlobalOffset  uint64
}

// New creates or opens an AppendOnlyLog in the specified directory.
func New(dir string) (*AppendOnlyLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	l := &AppendOnlyLog{
		Dir:      dir,
		Segments: make([]*Segment, 0),
	}

	if err := l.loadSegments(); err != nil {
		return nil, err
	}

	return l, nil
}

// loadSegments scans the directory for segment files and reconstructs the state.
func (l *AppendOnlyLog) loadSegments() error {
	entries, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		baseOffsetStr := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "segment-"), ".log")
		baseOffset, err := strconv.ParseUint(baseOffsetStr, 10, 64)
		if err != nil {
			continue // Skip invalid files
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		seg := &Segment{
			BaseOffset: baseOffset,
			FilePath:   filepath.Join(l.Dir, entry.Name()),
			Size:       info.Size(),
		}
		l.Segments = append(l.Segments, seg)
	}

	// Sort segments by BaseOffset
	sort.Slice(l.Segments, func(i, j int) bool {
		return l.Segments[i].BaseOffset < l.Segments[j].BaseOffset
	})

	// Initialize GlobalOffset and ActiveSegment
	if len(l.Segments) > 0 {
		lastSeg := l.Segments[len(l.Segments)-1]
		file, err := os.OpenFile(lastSeg.FilePath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		lastSeg.File = file
		l.ActiveSegment = lastSeg

		// Scan the last segment to find the next GlobalOffset
		offset, err := l.findNextOffset(lastSeg)
		if err != nil {
			return err
		}
		l.GlobalOffset = offset
	} else {
		l.GlobalOffset = 0
	}

	return nil
}

// findNextOffset reads the segment to determine the next available offset.
func (l *AppendOnlyLog) findNextOffset(seg *Segment) (uint64, error) {
	if seg.Size == 0 {
		return seg.BaseOffset, nil
	}

	if _, err := seg.File.Seek(0, 0); err != nil {
		return 0, err
	}

	var lastOffset uint64 = seg.BaseOffset
	decoder := NewSegmentDecoder(seg.File)

	for {
		msg, err := decoder.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		lastOffset = msg.Offset + 1
	}

	// Reset seek to end for appending
	if _, err := seg.File.Seek(0, 2); err != nil {
		return 0, err
	}

	return lastOffset, nil
}

// rotate creates a new segment file.
func (l *AppendOnlyLog) rotate() error {
	if l.ActiveSegment != nil {
		if err := l.ActiveSegment.File.Close(); err != nil {
			return err
		}
		l.ActiveSegment.File = nil
	}

	filename := fmt.Sprintf("segment-%020d.log", l.GlobalOffset)
	path := filepath.Join(l.Dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	newSeg := &Segment{
		BaseOffset: l.GlobalOffset,
		FilePath:   path,
		File:       file,
		Size:       0,
	}

	l.ActiveSegment = newSeg
	l.Segments = append(l.Segments, newSeg)
	return nil
}

// Append writes a message to the log.
func (l *AppendOnlyLog) Append(msg *message.Message) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.ActiveSegment == nil {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	msg.Offset = l.GlobalOffset
	
	// Serialize using MessagePack
	data, err := msg.Serialize()
	if err != nil {
		return 0, err
	}

	// Prepare buffer: 4 bytes length + data
	totalLen := 4 + len(data)
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)

	if l.ActiveSegment.Size+int64(totalLen) > MaxSegmentSize {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := l.ActiveSegment.File.Write(buf)
	if err != nil {
		return 0, err
	}

	l.ActiveSegment.Size += int64(n)
	l.GlobalOffset++

	return msg.Offset, nil
}

// Read retrieves messages starting from the given offset.
func (l *AppendOnlyLog) Read(offset uint64, max int) ([]*message.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var msgs []*message.Message
	
	startIdx := -1
	for i, seg := range l.Segments {
		nextBase := ^uint64(0) // Max uint64
		if i+1 < len(l.Segments) {
			nextBase = l.Segments[i+1].BaseOffset
		}

		if offset >= seg.BaseOffset && offset < nextBase {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		return msgs, nil
	}

	for i := startIdx; i < len(l.Segments); i++ {
		seg := l.Segments[i]
		
		f, err := os.Open(seg.FilePath)
		if err != nil {
			return nil, err
		}
		
		decoder := NewSegmentDecoder(f)
		for {
			m, err := decoder.Decode()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, err // Stop on error
			}

			if m.Offset >= offset {
				msgs = append(msgs, m)
				if len(msgs) >= max {
					f.Close()
					return msgs, nil
				}
			}
		}
		f.Close()
	}

	return msgs, nil
}

// SegmentDecoder helps reading messages from a stream
type SegmentDecoder struct {
	r io.Reader
}

func NewSegmentDecoder(r io.Reader) *SegmentDecoder {
	return &SegmentDecoder{r: r}
}

func (d *SegmentDecoder) Decode() (*message.Message, error) {
	// Read length prefix (4 bytes)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(d.r, lenBuf); err != nil {
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(lenBuf)
	
	// Read payload
	payloadBuf := make([]byte, payloadLen)
	if _, err := io.ReadFull(d.r, payloadBuf); err != nil {
		return nil, err
	}
	
	return message.Deserialize(payloadBuf)
}
