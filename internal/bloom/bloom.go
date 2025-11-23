package bloom

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type Filter struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
	path   string
	logger *slog.Logger
}

// load the bloom filter from disk or create a new one
func Load(path string, expectedItems uint, falsePositiveRate float64, logger *slog.Logger) (*Filter, error) {
	file, err := os.Open(path)
	if err == nil {
		defer file.Close()
		bf := bloom.NewWithEstimates(expectedItems, falsePositiveRate)
		if _, err := bf.ReadFrom(file); err != nil {
			logger.Warn("failed to read bloom filter, creating new one",
				slog.String("error", err.Error()))
			return &Filter{
				filter: bloom.NewWithEstimates(expectedItems, falsePositiveRate),
				path:   path,
				logger: logger,
			}, nil
		}
		logger.Info("loaded bloom filter from disk", slog.String("path", path))
		return &Filter{
			filter: bf,
			path:   path,
			logger: logger,
		}, nil
	}

	logger.Info("creating new bloom filter",
		slog.Uint64("capacity", uint64(expectedItems)),
		slog.Float64("false_positive_rate", falsePositiveRate*100))

	return &Filter{
		filter: bloom.NewWithEstimates(expectedItems, falsePositiveRate),
		path:   path,
		logger: logger,
	}, nil
}

func (f *Filter) Test(data []byte) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.filter.Test(data)
}

func (f *Filter) Add(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter.Add(data)
}

func (f *Filter) Save() error {
	tmpFile := f.path + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	f.mu.RLock()
	_, err = f.filter.WriteTo(file)
	f.mu.RUnlock()

	if err != nil {
		file.Close()
		return fmt.Errorf("write bloom filter: %w", err)
	}

	file.Close()

	if err := os.Rename(tmpFile, f.path); err != nil {
		return fmt.Errorf("rename bloom filter: %w", err)
	}

	f.logger.Debug("saved bloom filter", slog.String("path", f.path))
	return nil
}
