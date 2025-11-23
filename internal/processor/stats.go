package processor

import (
	"log/slog"
	"time"
)

// PrintProgress outputs current processing statistics
func (s *Stats) PrintProgress(logger *slog.Logger) {
	elapsed := time.Since(s.StartTime)
	listed := s.FilesListed.Load()
	downloaded := s.FilesDownloaded.Load()
	processed := s.FilesProcessed.Load()
	events := s.EventsProcessed.Load()
	written := s.EventsWritten.Load()
	duplicate := s.EventsDuplicate.Load()
	bytes := s.BytesDownloaded.Load()
	jsonlFiles := s.JSONLFilesWritten.Load()
	errors := s.Errors.Load()

	if elapsed.Seconds() > 0 {
		downloadRate := float64(downloaded) / elapsed.Seconds()
		eventRate := float64(events) / elapsed.Seconds()
		mbps := float64(bytes) / elapsed.Seconds() / 1024 / 1024

		logger.Info("progress",
			slog.Duration("elapsed", elapsed.Round(time.Second)),
			slog.Int64("files_listed", listed),
			slog.Int64("files_downloaded", downloaded),
			slog.Float64("download_rate", downloadRate),
			slog.Float64("mbps", mbps),
			slog.Int64("files_processed", processed),
			slog.Int64("events_total", events),
			slog.Float64("event_rate", eventRate),
			slog.Int64("events_written", written),
			slog.Int64("jsonl_files", jsonlFiles),
			slog.Int64("events_duplicate", duplicate),
			slog.Int64("errors", errors))
	}
}
