package logstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pulse/pkg/message"
)

// Segment represents a single log file on disk.
type Segment struct {
	BaseOffset uint64   // The offset of the first message in this segment
	FilePath   string   // Absolute path to the segment file
	File       *os.File // File handle (only open for the active segment)
	Size       int64    // Current size of the segment in bytes
}

// AppendOnlyLog manages the persistent log with rotating segments.
// It ensures messages are written sequentially and split across files.
type AppendOnlyLog struct {
	Dir           string       // Directory where segment files are stored
	mu            sync.RWMutex // Mutex for thread-safe access
	ActiveSegment *Segment     // The current segment being written to
	Segments      []*Segment   // List of all segments, sorted by BaseOffset
	GlobalOffset  uint64       // The next offset to be assigned to a message
	TotalSize     int64        // Total size of all segments in bytes

	// Configuration
	maxSegmentSize int64

	// Buffering and Flush
	bufWriter      *bufio.Writer
	flushInterval  time.Duration
	flushThreshold int
	unflushedCount int
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// Config holds configuration for the LogStore.
type Config struct {
	FlushInterval  time.Duration
	FlushThreshold int
	MaxSegmentSize int64
}

// New creates or opens an AppendOnlyLog in the specified directory.
// It recovers the state from existing files on disk.
func New(dir string, config Config) (*AppendOnlyLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Default to 128MB if not set
	maxSegSize := config.MaxSegmentSize
	if maxSegSize <= 0 {
		maxSegSize = 128 * 1024 * 1024
	}

	l := &AppendOnlyLog{
		Dir:            dir,
		Segments:       make([]*Segment, 0),
		flushInterval:  config.FlushInterval,
		flushThreshold: config.FlushThreshold,
		maxSegmentSize: maxSegSize,
		stopChan:       make(chan struct{}),
	}

	if err := l.loadSegments(); err != nil {
		return nil, err
	}

	// Start background flusher if interval is set
	if l.flushInterval > 0 {
		l.wg.Add(1)
		go l.runFlusher()
	}

	return l, nil
}

// GetGlobalOffset returns the next offset to be written.
func (l *AppendOnlyLog) GetGlobalOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.GlobalOffset
}

// loadSegments scans the directory for segment files and reconstructs the in-memory state.
func (l *AppendOnlyLog) loadSegments() error {
	entries, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		// Parse base offset from filename (e.g., "segment-00000000000000000001.log")
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
		l.TotalSize += info.Size()
	}

	// Sort segments by BaseOffset to ensure correct order
	sort.Slice(l.Segments, func(i, j int) bool {
		return l.Segments[i].BaseOffset < l.Segments[j].BaseOffset
	})

	// Initialize GlobalOffset and ActiveSegment
	if len(l.Segments) > 0 {
		lastSeg := l.Segments[len(l.Segments)-1]

		// Open the last segment for appending
		file, err := os.OpenFile(lastSeg.FilePath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		lastSeg.File = file
		l.ActiveSegment = lastSeg
		l.bufWriter = bufio.NewWriter(file)

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
// This is used during recovery to ensure we don't overwrite existing messages.
func (l *AppendOnlyLog) findNextOffset(seg *Segment) (uint64, error) {
	if seg.Size == 0 {
		return seg.BaseOffset, nil
	}

	// Seek to start to read the whole file
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

	// Reset seek to end for appending new messages
	if _, err := seg.File.Seek(0, 2); err != nil {
		return 0, err
	}

	return lastOffset, nil
}

// rotate creates a new segment file when the current one is full.
func (l *AppendOnlyLog) rotate() error {
	// Flush and close the current active segment
	if l.ActiveSegment != nil {
		if l.bufWriter != nil {
			if err := l.bufWriter.Flush(); err != nil {
				return err
			}
		}
		if err := l.ActiveSegment.File.Close(); err != nil {
			return err
		}
		l.ActiveSegment.File = nil // Release file handle
		l.bufWriter = nil
	}

	// Create new segment filename based on the current GlobalOffset
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
	l.bufWriter = bufio.NewWriter(file)
	return nil
}

// Append writes a message to the log.
// It handles serialization, segment rotation, and offset assignment.
func (l *AppendOnlyLog) Append(msg *message.Message) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Initialize first segment if none exists
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
	// We use a length prefix to know how many bytes to read during deserialization
	totalLen := 4 + len(data)
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)

	// Check if we need to rotate
	if l.ActiveSegment.Size+int64(totalLen) > l.maxSegmentSize {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	// Write to buffer
	// Performance vs Durability:
	// Writing to a buffer significantly improves performance by reducing syscalls (write).
	// However, it introduces a risk of data loss if the process crashes before the buffer is flushed to disk.
	// We mitigate this by flushing periodically (time-based) or after a certain number of messages (count-based).
	n, err := l.bufWriter.Write(buf)
	if err != nil {
		return 0, err
	}

	// Update state
	l.ActiveSegment.Size += int64(n)
	l.TotalSize += int64(n)
	l.GlobalOffset++

	// Check flush threshold
	l.unflushedCount++
	if l.flushThreshold > 0 && l.unflushedCount >= l.flushThreshold {
		if err := l.flush(); err != nil {
			return 0, err
		}
	}

	return msg.Offset, nil
}

// flush writes buffered data to the OS and syncs to disk.
// Must be called with lock held.
func (l *AppendOnlyLog) flush() error {
	if l.bufWriter == nil {
		return nil
	}
	if err := l.bufWriter.Flush(); err != nil {
		return err
	}
	// Sync ensures data is written to physical disk
	if l.ActiveSegment != nil && l.ActiveSegment.File != nil {
		if err := l.ActiveSegment.File.Sync(); err != nil {
			return err
		}
	}
	l.unflushedCount = 0
	return nil
}

// runFlusher periodically flushes the log.
func (l *AppendOnlyLog) runFlusher() {
	defer l.wg.Done()
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopChan:
			// Final flush before exit
			l.mu.Lock()
			l.flush()
			l.mu.Unlock()
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.unflushedCount > 0 {
				if err := l.flush(); err != nil {
					fmt.Printf("Error flushing log: %v\n", err)
				}
			}
			l.mu.Unlock()
		}
	}
}

// Close stops the background flusher and closes the active segment.
func (l *AppendOnlyLog) Close() error {
	close(l.stopChan)
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.ActiveSegment != nil {
		if l.bufWriter != nil {
			l.bufWriter.Flush()
		}
		if err := l.ActiveSegment.File.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Read retrieves messages starting from the given offset.
// It seamlessly reads across multiple segments.
func (l *AppendOnlyLog) Read(offset uint64, max int) ([]*message.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var msgs []*message.Message

	// Find the starting segment using binary search or linear scan
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
		return msgs, nil // Offset not found (too old or future)
	}

	// Iterate through segments starting from startIdx
	for i := startIdx; i < len(l.Segments); i++ {
		seg := l.Segments[i]

		// Open the segment file for reading
		f, err := os.Open(seg.FilePath)
		if err != nil {
			return nil, err
		}

		decoder := NewSegmentDecoder(bufio.NewReader(f))
		for {
			m, err := decoder.Decode()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, err // Stop on error (corruption?)
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

// RunRetention applies retention policies to the log.
// maxBytes: maximum total size of the log in bytes (0 = infinite).
// maxAge: maximum age of messages in the log (0 = infinite).
func (l *AppendOnlyLog) RunRetention(maxBytes int64, maxAge time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Size-based retention
	if maxBytes > 0 {
		for len(l.Segments) > 1 && l.TotalSize > maxBytes {
			// Delete the oldest segment
			oldest := l.Segments[0]

			// Safety check: never delete the active segment
			if oldest == l.ActiveSegment {
				break
			}

			if err := l.deleteSegment(oldest); err != nil {
				return err
			}

			// Remove from slice
			l.Segments = l.Segments[1:]
		}
	}

	// 2. Time-based retention
	if maxAge > 0 {
		now := time.Now()
		for len(l.Segments) > 1 {
			oldest := l.Segments[0]

			if oldest == l.ActiveSegment {
				break
			}

			// Check modification time of the file
			info, err := os.Stat(oldest.FilePath)
			if err != nil {
				fmt.Printf("Error stating segment %s: %v\n", oldest.FilePath, err)
				break
			}

			// If the file was last modified before the cutoff, all messages in it are old.
			if now.Sub(info.ModTime()) > maxAge {
				if err := l.deleteSegment(oldest); err != nil {
					return err
				}
				l.Segments = l.Segments[1:]
			} else {
				// Segments are ordered by time, so if this one is new enough, subsequent ones are too.
				break
			}
		}
	}

	return nil
}

// deleteSegment closes (if open) and removes the segment file.
func (l *AppendOnlyLog) deleteSegment(seg *Segment) error {
	if seg.File != nil {
		seg.File.Close()
	}

	if err := os.Remove(seg.FilePath); err != nil {
		if os.IsNotExist(err) {
			// Already removed by another routine; treat as success
			return nil
		}
		return fmt.Errorf("failed to delete segment %s: %w", seg.FilePath, err)
	}

	l.TotalSize -= seg.Size
	fmt.Printf("Deleted segment: %s (freed %d bytes)\n", filepath.Base(seg.FilePath), seg.Size)
	return nil
}

// SegmentDecoder helps reading messages from a stream
type SegmentDecoder struct {
	r io.Reader
}

func NewSegmentDecoder(r io.Reader) *SegmentDecoder {
	return &SegmentDecoder{r: r}
}

// Decode reads the next message from the stream.
// It first reads the 4-byte length prefix, then the MessagePack payload.
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
