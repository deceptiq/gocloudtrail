# gocloudtrail

Sync AWS CloudTrail logs from S3 to local JSONL files. Handles deduplication, resumption, and parallel processing across accounts/regions.

## Features

- Generate config from CloudTrail API or specify trails manually
- SQLite state tracking - resume from where you left off
- Bloom filter deduplication across multiple trails
- Parallel processing per account/region
- S3 Delimiter discovery - only checks regions with actual data

## Installation

```bash
go install github.com/deceptiq/gocloudtrail@latest
```

## Quick Start

Generate config from existing CloudTrail trails:

```bash
gocloudtrail generate-config config.json
```

Run the processor:

```bash
gocloudtrail run -config config.json
```

## Configuration

Generate config automatically or create it manually. Example:

```json
{
  "download_workers": 50, // parallel downloads (I/O bound)
  "process_workers": 0, // parallel processing (CPU bound, 0 = auto 2*CPUs)
  "download_queue_size": 5000, // download queue depth
  "process_queue_size": 2000, // processing queue depth
  "list_batch_size": 1000, // S3 ListObjects batch size
  "events_per_file": 10000, // events per output JSONL file

  "state_db": "state.db", // SQLite resumption state
  "bloom_file": "bloom.gob", // bloom filter for deduplication
  "events_dir": "events", // output directory

  "bloom_expected_items": 100000000, // expected total events
  "bloom_false_positive": 0.001, // bloom filter false positive rate

  "state_save_interval": 300, // save state every N seconds
  "progress_interval": 10, // print progress every N seconds
  "jsonl_flush_interval": 30, // flush JSONL buffers every N seconds

  "max_idle_conns": 500, // HTTP connection pool settings
  "max_idle_conns_per_host": 500,
  "max_conns_per_host": 500,
  "idle_conn_timeout": 90,
  "dial_timeout": 10,
  "keep_alive": 30,
  "client_timeout": 60,

  "trails": [
    {
      "name": "my-trail",
      "bucket": "my-cloudtrail-bucket",
      "prefix": "optional-prefix"
    }
  ]
}
```

## How It Works

1. Uses S3 Delimiter to find which account/region combinations have data
2. Tracks last processed S3 key per (bucket, account, region) in SQLite
3. Parallel workers download and decompress .json.gz files
4. Bloom filter checks event IDs to skip duplicates across trails
5. Writes JSONL files organized by account/region/date

State checkpoints every 100 files. Hit Ctrl+C to stop gracefully, then restart with the same config to resume.

## Permissions

Need `s3:ListBucket` and `s3:GetObject` on the CloudTrail bucket(s). Add `cloudtrail:DescribeTrails` if using `generate-config`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": "*"
    }
  ]
}
```
