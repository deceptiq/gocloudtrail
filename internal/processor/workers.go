package processor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (p *Processor) downloadWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range p.downloadJobs {
		resp, err := p.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(job.Bucket),
			Key:    aws.String(job.Key),
		})
		if err != nil {
			p.stats.Errors.Add(1)
			p.logger.Error("failed to download object",
				slog.String("bucket", job.Bucket),
				slog.String("key", job.Key),
				slog.String("error", err.Error()))
			continue
		}

		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if err != nil {
			p.stats.Errors.Add(1)
			p.logger.Error("failed to read object",
				slog.String("bucket", job.Bucket),
				slog.String("key", job.Key),
				slog.String("error", err.Error()))
			continue
		}

		p.stats.FilesDownloaded.Add(1)
		p.stats.BytesDownloaded.Add(int64(len(data)))

		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			p.stats.Errors.Add(1)
			p.logger.Error("failed to decompress object",
				slog.String("bucket", job.Bucket),
				slog.String("key", job.Key),
				slog.String("error", err.Error()))
			continue
		}

		var logFile CloudTrailLogFile
		if err := json.NewDecoder(gr).Decode(&logFile); err != nil {
			_ = gr.Close()
			p.stats.Errors.Add(1)
			p.logger.Error("failed to parse JSON",
				slog.String("bucket", job.Bucket),
				slog.String("key", job.Key),
				slog.String("error", err.Error()))
			continue
		}
		_ = gr.Close()

		p.processJobs <- ProcessedFile{
			Job:     job,
			Records: logFile.Records,
		}
	}
}

// process CloudTrail log files into JSONL files
func (p *Processor) processWorker(wg *sync.WaitGroup) {
	defer wg.Done()

	for file := range p.processJobs {
		if file.Err != nil {
			continue
		}

		for _, rawEvent := range file.Records {
			p.stats.EventsProcessed.Add(1)

			// parse minimal fields for deduplication
			var minimal MinimalEvent
			if err := json.Unmarshal(rawEvent, &minimal); err != nil {
				continue
			}

			// check bloom filter for duplicates
			if p.bloomFilter.Test([]byte(minimal.EventID)) {
				p.stats.EventsDuplicate.Add(1)
				continue
			}

			// parse event time
			eventTime, err := time.Parse(time.RFC3339, minimal.EventTime)
			if err != nil {
				continue
			}

			// determine account ID
			accountID := minimal.RecipientAccountID
			if accountID == "" {
				accountID = minimal.UserIdentity.AccountID
			}
			if accountID == "" {
				continue
			}

			// write to JSONL
			if err := p.jsonlWriter.Write(accountID, minimal.AWSRegion, eventTime, rawEvent); err != nil {
				p.logger.Error("failed to write event to JSONL",
					slog.String("error", err.Error()))
				continue
			}

			// add to bloom filter
			p.bloomFilter.Add([]byte(minimal.EventID))

			p.stats.EventsWritten.Add(1)
		}

		p.stats.FilesProcessed.Add(1)
	}
}

func (p *Processor) progressReporter(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.stats.PrintProgress(p.logger)
		}
	}
}

func (p *Processor) jsonlFlusher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.jsonlWriter.FlushAll(); err != nil {
				p.logger.Error("failed to flush JSONL buffers",
					slog.String("error", err.Error()))
			}
			p.stats.JSONLFilesWritten.Store(int64(p.jsonlWriter.BufferCount()))
		}
	}
}

func (p *Processor) bloomSaver(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.bloomFilter.Save(); err != nil {
				p.logger.Error("failed to save bloom filter",
					slog.String("error", err.Error()))
			}
		}
	}
}
