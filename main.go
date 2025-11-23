package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/deceptiq/gocloudtrail/internal/bloom"
	"github.com/deceptiq/gocloudtrail/internal/processor"
	"github.com/deceptiq/gocloudtrail/internal/state"
)

const (
	stateDB                = "state.db"
	bloomFile              = "bloom.gob"
	eventsDir              = "events"
	maxDownloadConcurrency = 50
	maxProcessConcurrency  = 0 // Auto-set to NumCPU * 2
	downloadQueueSize      = 5000
	processQueueSize       = 2000
	listBatchSize          = 1000
	stateSaveInterval      = 5 * time.Minute
	progressInterval       = 10 * time.Second
	eventsPerJSONLFile     = 10000
	jsonlFlushInterval     = 30 * time.Second
	bloomExpectedItems     = 100_000_000
	bloomFalsePositive     = 0.001
	maxIdleConns           = 500
	maxIdleConnsPerHost    = 500
	maxConnsPerHost        = 500
	idleConnTimeout        = 90 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpClient := createHTTPClient()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	if err != nil {
		logger.Error("failed to load AWS config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Error("failed to get caller identity", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("authenticated with AWS", slog.String("account", aws.ToString(identity.Account)))

	if err := os.MkdirAll(eventsDir, 0o755); err != nil {
		logger.Error("failed to create events directory", slog.String("error", err.Error()))
		os.Exit(1)
	}

	numCPU := runtime.NumCPU()
	processConcurrency := numCPU * 2
	if maxProcessConcurrency > 0 {
		processConcurrency = maxProcessConcurrency
	}

	logger.Info("system configuration",
		slog.Int("cpu_cores", numCPU),
		slog.Int("download_workers", maxDownloadConcurrency),
		slog.Int("process_workers", processConcurrency))

	stateDB, err := state.Open(stateDB, logger)
	if err != nil {
		logger.Error("failed to open state database", slog.String("error", err.Error()))
		os.Exit(1)
	}

	bloomFilter, err := bloom.Load(bloomFile, bloomExpectedItems, bloomFalsePositive, logger)
	if err != nil {
		logger.Error("failed to load bloom filter", slog.String("error", err.Error()))
		os.Exit(1)
	}

	proc := processor.New(
		s3.NewFromConfig(cfg),
		cloudtrail.NewFromConfig(cfg),
		stateDB,
		bloomFilter,
		processor.Config{
			DownloadWorkers:   maxDownloadConcurrency,
			ProcessWorkers:    processConcurrency,
			DownloadQueueSize: downloadQueueSize,
			ProcessQueueSize:  processQueueSize,
			ListBatchSize:     listBatchSize,
			EventsPerFile:     eventsPerJSONLFile,
			EventsDir:         eventsDir,
		},
		logger,
	)

	if err := proc.Run(ctx, progressInterval, jsonlFlushInterval, stateSaveInterval); err != nil {
		if err == context.Canceled {
			logger.Info("received interrupt signal, shutting down gracefully")
		} else {
			logger.Error("processing failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}

	proc.Stats().PrintProgress(logger)
	logger.Info("processing complete")
}

func createHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxIdleConns,
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			MaxConnsPerHost:     maxConnsPerHost,
			IdleConnTimeout:     idleConnTimeout,
			DisableCompression:  true,
			ForceAttemptHTTP2:   true,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
		Timeout: 60 * time.Second,
	}
}
