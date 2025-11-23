package writer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type JSONLWriter struct {
	mu              sync.Mutex
	buffers         map[string]*eventBuffer
	eventsDir       string
	eventsPerFile   int
	nextFileCounter map[string]int
	logger          *slog.Logger
}

type eventBuffer struct {
	events []json.RawMessage
}

func New(eventsDir string, eventsPerFile int, logger *slog.Logger) *JSONLWriter {
	return &JSONLWriter{
		buffers:         make(map[string]*eventBuffer),
		eventsDir:       eventsDir,
		eventsPerFile:   eventsPerFile,
		nextFileCounter: make(map[string]int),
		logger:          logger,
	}
}

func (w *JSONLWriter) Write(accountID, region string, eventTime time.Time, rawEvent json.RawMessage) error {
	key := fmt.Sprintf("%s/%s/%s", accountID, region, eventTime.Format("2006/01/02/15"))

	w.mu.Lock()
	defer w.mu.Unlock()

	buf, exists := w.buffers[key]
	if !exists {
		buf = &eventBuffer{
			events: make([]json.RawMessage, 0, w.eventsPerFile),
		}
		w.buffers[key] = buf
	}

	buf.events = append(buf.events, rawEvent)

	if len(buf.events) >= w.eventsPerFile {
		return w.flushBufferLocked(key, buf)
	}

	return nil
}

func (w *JSONLWriter) flushBufferLocked(key string, buf *eventBuffer) error {
	if len(buf.events) == 0 {
		return nil
	}

	counter := w.nextFileCounter[key]
	w.nextFileCounter[key]++

	filePath := filepath.Join(w.eventsDir, key, fmt.Sprintf("events_%05d.jsonl", counter))

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() { _ = f.Close() }()

	writer := bufio.NewWriter(f)
	for _, event := range buf.events {
		if _, err := writer.Write(event); err != nil {
			return fmt.Errorf("write event: %w", err)
		}
		if err := writer.WriteByte('\n'); err != nil {
			return fmt.Errorf("write newline: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	w.logger.Debug("flushed buffer",
		slog.String("key", key),
		slog.Int("events", len(buf.events)),
		slog.String("file", filePath))

	buf.events = buf.events[:0]
	return nil
}

func (w *JSONLWriter) FlushAll() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for key, buf := range w.buffers {
		if err := w.flushBufferLocked(key, buf); err != nil {
			w.logger.Error("failed to flush buffer",
				slog.String("key", key),
				slog.String("error", err.Error()))
		}
	}

	return nil
}

func (w *JSONLWriter) BufferCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buffers)
}
