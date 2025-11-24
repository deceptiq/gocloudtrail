package processor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// find all AWS accounts in the S3 bucket structure (no need for organization discovery)
func (p *Processor) discoverAccounts(ctx context.Context, bucket, basePrefix string) ([]string, string) {
	var orgID string
	accountMap := make(map[string]bool)

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(basePrefix),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(100),
	}

	resp, err := p.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		p.logger.Error("failed to discover accounts", slog.String("error", err.Error()))
		return nil, ""
	}

	for _, prefix := range resp.CommonPrefixes {
		parts := strings.Split(aws.ToString(prefix.Prefix), "/")
		if len(parts) >= 2 {
			id := parts[len(parts)-2]

			// Check if this is an AWS Organization
			if strings.HasPrefix(id, "o-") {
				orgID = id
				orgPrefix := basePrefix + id + "/"
				orgInput := &s3.ListObjectsV2Input{
					Bucket:    aws.String(bucket),
					Prefix:    aws.String(orgPrefix),
					Delimiter: aws.String("/"),
					MaxKeys:   aws.Int32(1000),
				}

				orgResp, err := p.s3Client.ListObjectsV2(ctx, orgInput)
				if err != nil {
					p.logger.Error("failed to list organization accounts",
						slog.String("error", err.Error()))
					continue
				}

				for _, orgPfx := range orgResp.CommonPrefixes {
					orgParts := strings.Split(aws.ToString(orgPfx.Prefix), "/")
					if len(orgParts) >= 3 {
						accountMap[orgParts[len(orgParts)-2]] = true
					}
				}
			} else if len(id) == 12 && isNumeric(id) {
				accountMap[id] = true
			}
		}
	}

	accounts := make([]string, 0, len(accountMap))
	for account := range accountMap {
		accounts = append(accounts, account)
	}

	return accounts, orgID
}

// AccountRegionPair represents an account/region combination that has data
type AccountRegionPair struct {
	AccountID string
	Region    string
}

// discoverAccountRegions finds all account/region combinations that actually have CloudTrail logs
func (p *Processor) discoverAccountRegions(ctx context.Context, bucket, basePrefix string, accounts []string, orgID string) []AccountRegionPair {
	var pairs []AccountRegionPair
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, accountID := range accounts {
		wg.Add(1)
		go func(acct string) {
			defer wg.Done()

			var prefix string
			if orgID != "" {
				prefix = fmt.Sprintf("%s%s/%s/CloudTrail/", basePrefix, orgID, acct)
			} else {
				prefix = fmt.Sprintf("%s%s/CloudTrail/", basePrefix, acct)
			}

			input := &s3.ListObjectsV2Input{
				Bucket:    aws.String(bucket),
				Prefix:    aws.String(prefix),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(1000),
			}

			paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)
			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					p.logger.Error("failed to discover regions",
						slog.String("account", acct),
						slog.String("error", err.Error()))
					break
				}

				for _, commonPrefix := range page.CommonPrefixes {
					parts := strings.Split(aws.ToString(commonPrefix.Prefix), "/")
					for i, part := range parts {
						if part == "CloudTrail" && i+1 < len(parts) {
							region := parts[i+1]
							if region != "" {
								mu.Lock()
								pairs = append(pairs, AccountRegionPair{
									AccountID: acct,
									Region:    region,
								})
								mu.Unlock()
							}
							break
						}
					}
				}
			}
		}(accountID)
	}
	wg.Wait()

	return pairs
}

func (p *Processor) processAccountRegion(ctx context.Context, bucket, basePrefix, accountID, region, orgID string) {
	stateKey := fmt.Sprintf("%s:%s:%s", bucket, accountID, region)

	// Check for resumption state
	lastKey, err := p.stateDB.GetLastProcessedKey(bucket, accountID, region)
	if err != nil {
		p.logger.Error("failed to get last processed key",
			slog.String("state_key", stateKey),
			slog.String("error", err.Error()))
	}
	if lastKey != "" {
		p.logger.Info("resuming from last checkpoint",
			slog.String("state_key", stateKey),
			slog.String("last_key", lastKey))
	}

	// Build S3 prefix
	var searchPrefix string
	if orgID != "" {
		searchPrefix = fmt.Sprintf("%s%s/%s/CloudTrail/%s/", basePrefix, orgID, accountID, region)
	} else {
		searchPrefix = fmt.Sprintf("%s%s/CloudTrail/%s/", basePrefix, accountID, region)
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(searchPrefix),
		MaxKeys: aws.Int32(int32(p.config.ListBatchSize)),
	}

	if lastKey != "" {
		input.StartAfter = aws.String(lastKey)
	}

	filesListed := 0
	var lastSeenKey string
	paginator := s3.NewListObjectsV2Paginator(p.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			p.logger.Error("failed to list objects",
				slog.String("state_key", stateKey),
				slog.String("error", err.Error()))
			p.stats.Errors.Add(1)
			return
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)

			if !strings.HasSuffix(key, ".json.gz") {
				continue
			}

			p.stats.FilesListed.Add(1)
			filesListed++
			lastSeenKey = key

			p.downloadJobs <- DownloadJob{
				Bucket:       bucket,
				Key:          key,
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			}

			// Periodically save progress
			if filesListed%100 == 0 {
				if err := p.stateDB.UpdateLastProcessedKey(bucket, accountID, region, key); err != nil {
					p.logger.Error("failed to update state",
						slog.String("state_key", stateKey),
						slog.String("error", err.Error()))
				}
			}
		}
	}

	// Save final state (critical for account/regions with < 100 files)
	if filesListed > 0 {
		if err := p.stateDB.UpdateLastProcessedKey(bucket, accountID, region, lastSeenKey); err != nil {
			p.logger.Error("failed to save final state",
				slog.String("state_key", stateKey),
				slog.String("error", err.Error()))
		}
		p.logger.Info("enqueued files",
			slog.String("state_key", stateKey),
			slog.Int("count", filesListed))
	}
}
