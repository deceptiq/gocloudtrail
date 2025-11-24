package state

import (
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/mattn/go-sqlite3"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS state (
	bucket TEXT NOT NULL,
	account_id TEXT NOT NULL,
	region TEXT NOT NULL,
	last_processed_key TEXT,
	processed_count INTEGER DEFAULT 0,
	last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (bucket, account_id, region)
)`

type DB struct {
	db     *sql.DB
	logger *slog.Logger
}

func Open(path string, logger *slog.Logger) (*DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if _, err = db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}

	logger.Info("initialized state database", slog.String("path", path))

	return &DB{db: db, logger: logger}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) GetLastProcessedKey(bucket, accountID, region string) (string, error) {
	var lastKey sql.NullString
	err := d.db.QueryRow(
		"SELECT last_processed_key FROM state WHERE bucket = ? AND account_id = ? AND region = ?",
		bucket, accountID, region,
	).Scan(&lastKey)

	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query last key: %w", err)
	}

	if lastKey.Valid {
		return lastKey.String, nil
	}
	return "", nil
}

func (d *DB) UpdateLastProcessedKey(bucket, accountID, region, key string) error {
	_, err := d.db.Exec(`
		INSERT INTO state (bucket, account_id, region, last_processed_key, processed_count, last_updated)
		VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
		ON CONFLICT(bucket, account_id, region) DO UPDATE SET
			last_processed_key = excluded.last_processed_key,
			processed_count = processed_count + 1,
			last_updated = CURRENT_TIMESTAMP
	`, bucket, accountID, region, key)
	if err != nil {
		return fmt.Errorf("update state: %w", err)
	}

	return nil
}
