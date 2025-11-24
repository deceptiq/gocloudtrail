package main

import (
	"context"
	"flag"
	"fmt"
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
	appConfig "github.com/deceptiq/gocloudtrail/internal/config"
	"github.com/deceptiq/gocloudtrail/internal/processor"
	"github.com/deceptiq/gocloudtrail/internal/state"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate-config":
		runGenerateConfig(logger)
	case "run":
		runProcessor(logger)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  generate-config <output-path>  Generate config.json from CloudTrail API\n")
	fmt.Fprintf(os.Stderr, "  run -config <path>             Run the CloudTrail processor\n")
}

func runGenerateConfig(logger *slog.Logger) {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s generate-config <output-path>\n", os.Args[0])
		os.Exit(1)
	}
	if err := appConfig.Generate(os.Args[2], logger); err != nil {
		logger.Error("failed to generate config", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func runProcessor(logger *slog.Logger) {
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := runCmd.String("config", "", "Path to config.json (required)")
	runCmd.Parse(os.Args[2:])

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -config flag is required\n")
		fmt.Fprintf(os.Stderr, "Usage: %s run -config <path>\n", os.Args[0])
		os.Exit(1)
	}

	appCfg, err := appConfig.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config file", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("loaded config from file", slog.String("path", *configPath))

	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpClient := createHTTPClient(appCfg)
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

	if err := os.MkdirAll(appCfg.EventsDir, 0o755); err != nil {
		logger.Error("failed to create events directory", slog.String("error", err.Error()))
		os.Exit(1)
	}

	numCPU := runtime.NumCPU()
	processConcurrency := numCPU * 2
	if appCfg.ProcessWorkers > 0 {
		processConcurrency = appCfg.ProcessWorkers
	}

	logger.Info("system configuration",
		slog.Int("cpu_cores", numCPU),
		slog.Int("download_workers", appCfg.DownloadWorkers),
		slog.Int("process_workers", processConcurrency))

	stateDB, err := state.Open(appCfg.StateDB, logger)
	if err != nil {
		logger.Error("failed to open state database", slog.String("error", err.Error()))
		os.Exit(1)
	}

	bloomFilter, err := bloom.Load(appCfg.BloomFile, uint(appCfg.BloomExpectedItems), appCfg.BloomFalsePositive, logger)
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
			DownloadWorkers:   appCfg.DownloadWorkers,
			ProcessWorkers:    processConcurrency,
			DownloadQueueSize: appCfg.DownloadQueueSize,
			ProcessQueueSize:  appCfg.ProcessQueueSize,
			ListBatchSize:     appCfg.ListBatchSize,
			EventsPerFile:     appCfg.EventsPerFile,
			EventsDir:         appCfg.EventsDir,
			Trails:            appCfg.Trails,
		},
		logger,
	)

	progressInterval := time.Duration(appCfg.ProgressInterval) * time.Second
	jsonlFlushInterval := time.Duration(appCfg.JSONLFlushInterval) * time.Second
	stateSaveInterval := time.Duration(appCfg.StateSaveInterval) * time.Second

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

func createHTTPClient(cfg *appConfig.Config) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        cfg.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			MaxConnsPerHost:     cfg.MaxConnsPerHost,
			IdleConnTimeout:     time.Duration(cfg.IdleConnTimeout) * time.Second,
			DisableCompression:  true,
			ForceAttemptHTTP2:   true,
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(cfg.DialTimeout) * time.Second,
				KeepAlive: time.Duration(cfg.KeepAlive) * time.Second,
			}).DialContext,
		},
		Timeout: time.Duration(cfg.ClientTimeout) * time.Second,
	}
}
