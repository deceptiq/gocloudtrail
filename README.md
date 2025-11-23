# gocloudtrail

Downloads and deduplicates AWS CloudTrail logs into JSONL files.

## Usage

Requires AWS credentials with `cloudtrail:DescribeTrails`, `s3:ListBucket`, `s3:GetObject`

## Algorithm

**Discovery:**

1. Call `DescribeTrails` to find all CloudTrail trails
2. For each trail, list S3 bucket prefixes to discover accounts (including AWS Organizations)
3. For each account, discover regions by listing `AWSLogs/{account}/CloudTrail/{region}/` prefixes

**Processing:**

1. For each account/region pair, check SQLite `state.db` for last processed S3 key
2. List S3 objects starting after last processed key (resumable)
3. Queue `.json.gz` files for download (50 concurrent workers)
4. Download, decompress, parse CloudTrail events
5. For each event, check bloom filter by `eventID` to skip duplicates
6. Write new events to JSONL files organized by `{account}/{region}/{YYYY}/{MM}/{DD}/{HH}/events_NNNNN.jsonl`
7. Every 100 files, update SQLite with current S3 key for resumption

**State:**

- `state.db` - SQLite table tracking `(account_id, region) -> last_processed_key`
- `bloom.gob` - Bloom filter of seen `eventID`s

**Running**

- Interrupt with Ctrl+C to stop gracefully.
- Restart to resume from last checkpoint.

## Configuration

Edit constants in `cmd/poller/main.go` for workers, batch sizes, and bloom filter capacity. All other information such as organization trail details will be auto-discovered.
