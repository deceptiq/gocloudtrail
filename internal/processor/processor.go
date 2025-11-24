package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/deceptiq/gocloudtrail/internal/bloom"
	"github.com/deceptiq/gocloudtrail/internal/config"
	"github.com/deceptiq/gocloudtrail/internal/state"
	"github.com/deceptiq/gocloudtrail/internal/writer"
)

type Config struct {
	DownloadWorkers   int
	ProcessWorkers    int
	DownloadQueueSize int
	ProcessQueueSize  int
	ListBatchSize     int
	EventsPerFile     int
	EventsDir         string
	Trails            []config.Trail
}

type Processor struct {
	s3Client     *s3.Client
	ctClient     *cloudtrail.Client
	stateDB      *state.DB
	bloomFilter  *bloom.Filter
	jsonlWriter  *writer.JSONLWriter
	stats        *Stats
	config       Config
	logger       *slog.Logger
	downloadJobs chan DownloadJob
	processJobs  chan ProcessedFile
}

func New(
	s3Client *s3.Client,
	ctClient *cloudtrail.Client,
	stateDB *state.DB,
	bloomFilter *bloom.Filter,
	config Config,
	logger *slog.Logger,
) *Processor {
	return &Processor{
		s3Client:     s3Client,
		ctClient:     ctClient,
		stateDB:      stateDB,
		bloomFilter:  bloomFilter,
		jsonlWriter:  writer.New(config.EventsDir, config.EventsPerFile, logger),
		stats:        &Stats{StartTime: time.Now()},
		config:       config,
		logger:       logger,
		downloadJobs: make(chan DownloadJob, config.DownloadQueueSize),
		processJobs:  make(chan ProcessedFile, config.ProcessQueueSize),
	}
}

// Run executes the processing pipeline
func (p *Processor) Run(ctx context.Context, progressInterval, flushInterval, bloomSaveInterval time.Duration) error {
	defer func() {
		p.logger.Info("flushing buffers and saving state")
		if err := p.jsonlWriter.FlushAll(); err != nil {
			p.logger.Error("failed to flush JSONL buffers", slog.String("error", err.Error()))
		}
		if err := p.bloomFilter.Save(); err != nil {
			p.logger.Error("failed to save bloom filter", slog.String("error", err.Error()))
		}
		_ = p.stateDB.Close()
		p.logger.Info("state saved successfully")
	}()

	// start background tasks
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go p.progressReporter(progressCtx, progressInterval)

	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go p.jsonlFlusher(flushCtx, flushInterval)

	bloomCtx, bloomCancel := context.WithCancel(ctx)
	defer bloomCancel()
	go p.bloomSaver(bloomCtx, bloomSaveInterval)

	// start downloader workers
	var downloadWg sync.WaitGroup
	for range p.config.DownloadWorkers {
		downloadWg.Add(1)
		go p.downloadWorker(ctx, &downloadWg)
	}

	// start processor workers
	var processWg sync.WaitGroup
	for range p.config.ProcessWorkers {
		processWg.Add(1)
		go p.processWorker(&processWg)
	}

	// discover and enqueue jobs
	if err := p.discoverAndProcess(ctx); err != nil {
		if ctx.Err() == context.Canceled {
			return context.Canceled
		}
		return err
	}

	// wait for pipeline to drain
	close(p.downloadJobs)
	downloadWg.Wait()

	close(p.processJobs)
	processWg.Wait()

	return nil
}

func (p *Processor) Stats() *Stats {
	return p.stats
}

func (p *Processor) discoverAndProcess(ctx context.Context) error {
	// If trails are provided in config, use those instead of API discovery
	if len(p.config.Trails) > 0 {
		p.logger.Info("processing trails from config", slog.Int("count", len(p.config.Trails)))

		var wg sync.WaitGroup
		for _, trail := range p.config.Trails {
			wg.Add(1)
			go func(t config.Trail) {
				defer wg.Done()
				p.processConfigTrail(ctx, t)
			}(trail)
		}

		wg.Wait()
		return nil
	}

	// Fall back to API discovery
	p.logger.Info("discovering CloudTrail trails via API")

	resp, err := p.ctClient.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})
	if err != nil {
		return fmt.Errorf("describe trails: %w", err)
	}

	p.logger.Info("discovered trails", slog.Int("count", len(resp.TrailList)))

	var wg sync.WaitGroup
	for _, trail := range resp.TrailList {
		wg.Add(1)
		go func(t types.Trail) {
			defer wg.Done()
			p.processTrail(ctx, t)
		}(trail)
	}

	wg.Wait()
	return nil
}

func (p *Processor) processConfigTrail(ctx context.Context, trail config.Trail) {
	trailName := trail.Name
	bucketName := trail.Bucket
	prefix := trail.Prefix

	p.logger.Info("processing trail",
		slog.String("trail", trailName),
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix))

	basePrefix := ""
	if prefix != "" {
		basePrefix = prefix + "/"
	}
	basePrefix += "AWSLogs/"

	// discover accounts
	accounts, orgID := p.discoverAccounts(ctx, bucketName, basePrefix)
	if orgID != "" {
		p.logger.Info("AWS Organization detected",
			slog.String("trail", trailName),
			slog.String("org_id", orgID))
	}
	p.logger.Info("discovered accounts",
		slog.String("trail", trailName),
		slog.Int("count", len(accounts)))

	// discover account/region pairs that actually have data
	pairs := p.discoverAccountRegions(ctx, bucketName, basePrefix, accounts, orgID)
	p.logger.Info("discovered account/region combinations with data",
		slog.String("trail", trailName),
		slog.Int("count", len(pairs)))

	// process only the account/region pairs that have data
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(pr AccountRegionPair) {
			defer wg.Done()
			p.processAccountRegion(ctx, bucketName, basePrefix, pr.AccountID, pr.Region, orgID)
		}(pair)
	}
	wg.Wait()

	p.logger.Info("finished processing trail", slog.String("trail", trailName))
}

func (p *Processor) processTrail(ctx context.Context, trail types.Trail) {
	trailName := aws.ToString(trail.Name)
	bucketName := aws.ToString(trail.S3BucketName)
	prefix := aws.ToString(trail.S3KeyPrefix)

	p.logger.Info("processing trail",
		slog.String("trail", trailName),
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix))

	basePrefix := ""
	if prefix != "" {
		basePrefix = prefix + "/"
	}
	basePrefix += "AWSLogs/"

	// discover accounts
	accounts, orgID := p.discoverAccounts(ctx, bucketName, basePrefix)
	if orgID != "" {
		p.logger.Info("AWS Organization detected",
			slog.String("trail", trailName),
			slog.String("org_id", orgID))
	}
	p.logger.Info("discovered accounts",
		slog.String("trail", trailName),
		slog.Int("count", len(accounts)))

	// discover account/region pairs that actually have data
	pairs := p.discoverAccountRegions(ctx, bucketName, basePrefix, accounts, orgID)
	p.logger.Info("discovered account/region combinations with data",
		slog.String("trail", trailName),
		slog.Int("count", len(pairs)))

	// process only the account/region pairs that have data
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(pr AccountRegionPair) {
			defer wg.Done()
			p.processAccountRegion(ctx, bucketName, basePrefix, pr.AccountID, pr.Region, orgID)
		}(pair)
	}
	wg.Wait()

	p.logger.Info("finished processing trail", slog.String("trail", trailName))
}

func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
