package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
)

type Trail struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix,omitempty"`
}

type Config struct {
	// Processing settings
	DownloadWorkers   int `json:"download_workers"`
	ProcessWorkers    int `json:"process_workers"`
	DownloadQueueSize int `json:"download_queue_size"`
	ProcessQueueSize  int `json:"process_queue_size"`
	ListBatchSize     int `json:"list_batch_size"`
	EventsPerFile     int `json:"events_per_file"`

	// Directories
	StateDB   string `json:"state_db"`
	BloomFile string `json:"bloom_file"`
	EventsDir string `json:"events_dir"`

	// Bloom filter settings
	BloomExpectedItems uint64  `json:"bloom_expected_items"`
	BloomFalsePositive float64 `json:"bloom_false_positive"`

	// Intervals (in seconds)
	StateSaveInterval  int `json:"state_save_interval"`
	ProgressInterval   int `json:"progress_interval"`
	JSONLFlushInterval int `json:"jsonl_flush_interval"`

	// HTTP client settings (in seconds)
	MaxIdleConns        int `json:"max_idle_conns"`
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
	MaxConnsPerHost     int `json:"max_conns_per_host"`
	IdleConnTimeout     int `json:"idle_conn_timeout"`
	DialTimeout         int `json:"dial_timeout"`
	KeepAlive           int `json:"keep_alive"`
	ClientTimeout       int `json:"client_timeout"`

	// Trails to process
	Trails []Trail `json:"trails"`
}

func Default() *Config {
	return &Config{
		DownloadWorkers:     50,
		ProcessWorkers:      0, // Auto-set to NumCPU * 2
		DownloadQueueSize:   5000,
		ProcessQueueSize:    2000,
		ListBatchSize:       1000,
		EventsPerFile:       10000,
		StateDB:             "state.db",
		BloomFile:           "bloom.gob",
		EventsDir:           "events",
		BloomExpectedItems:  100_000_000,
		BloomFalsePositive:  0.001,
		StateSaveInterval:   300, // 5 minutes
		ProgressInterval:    10,  // 10 seconds
		JSONLFlushInterval:  30,  // 30 seconds
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 500,
		MaxConnsPerHost:     500,
		IdleConnTimeout:     90, // seconds
		DialTimeout:         10, // seconds
		KeepAlive:           30, // seconds
		ClientTimeout:       60, // seconds
		Trails:              []Trail{},
	}
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := Default()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

func (c *Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}

	return nil
}

func Generate(outputPath string, logger *slog.Logger) error {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	ctClient := cloudtrail.NewFromConfig(cfg)

	logger.Info("discovering CloudTrail trails")
	resp, err := ctClient.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})
	if err != nil {
		return fmt.Errorf("describe trails: %w", err)
	}

	appCfg := Default()

	for _, trail := range resp.TrailList {
		appCfg.Trails = append(appCfg.Trails, Trail{
			Name:   aws.ToString(trail.Name),
			Bucket: aws.ToString(trail.S3BucketName),
			Prefix: aws.ToString(trail.S3KeyPrefix),
		})
	}

	logger.Info("discovered trails", slog.Int("count", len(appCfg.Trails)))

	if err := appCfg.Save(outputPath); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	logger.Info("config saved", slog.String("path", outputPath))
	return nil
}
