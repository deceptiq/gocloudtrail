package processor

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

// S3 object to download and process
type DownloadJob struct {
	Bucket       string
	Key          string
	Size         int64
	LastModified time.Time
}

// parsed records from a CloudTrail log file
type ProcessedFile struct {
	Job     DownloadJob
	Records []json.RawMessage
	Err     error
}

// only the fields needed for deduplication and routing
type MinimalEvent struct {
	EventTime    string `json:"eventTime"`
	EventID      string `json:"eventID"`
	AWSRegion    string `json:"awsRegion"`
	UserIdentity struct {
		AccountID string `json:"accountId"`
	} `json:"userIdentity"`
	RecipientAccountID string `json:"recipientAccountId,omitempty"`
}

// the structure of a CloudTrail log file
type CloudTrailLogFile struct {
	Records []json.RawMessage `json:"Records"`
}

// processing metrics
type Stats struct {
	FilesListed       atomic.Int64
	FilesDownloaded   atomic.Int64
	FilesProcessed    atomic.Int64
	EventsProcessed   atomic.Int64
	EventsWritten     atomic.Int64
	EventsDuplicate   atomic.Int64
	BytesDownloaded   atomic.Int64
	JSONLFilesWritten atomic.Int64
	Errors            atomic.Int64
	StartTime         time.Time
}
