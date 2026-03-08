package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	scheduler "github.com/DEEJ4Y/mongodb-cron"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds the configuration for the PostgreSQL job store.
type Config struct {
	// Pool is the pgx connection pool. Required.
	Pool *pgxpool.Pool

	// TableName is the name of the jobs table. Default: "jobs".
	TableName string

	// Condition is an optional extra WHERE clause fragment appended with AND.
	// Example: "data->>'type' = 'email'"
	Condition string

	// ConditionArgs are the positional arguments for Condition.
	// Parameter numbering in Condition should use $1, $2, etc.
	// They will be renumbered automatically when appended to the query.
	ConditionArgs []interface{}
}

// Store implements scheduler.JobStore for PostgreSQL using pgx/v5.
type Store struct {
	pool          *pgxpool.Pool
	tableName     string
	condition     string
	conditionArgs []interface{}
}

// NewStore creates a new PostgreSQL job store with the given configuration.
func NewStore(config Config) (*Store, error) {
	if config.Pool == nil {
		return nil, fmt.Errorf("pool is required")
	}

	if config.TableName == "" {
		config.TableName = "jobs"
	}

	return &Store{
		pool:          config.Pool,
		tableName:     config.TableName,
		condition:     config.Condition,
		conditionArgs: config.ConditionArgs,
	}, nil
}

// LockNext atomically finds and locks the next available job.
// It uses a CTE with FOR UPDATE SKIP LOCKED to prevent race conditions.
func (s *Store) LockNext(ctx context.Context, lockUntil time.Time) (*scheduler.Job, error) {
	// Build the query with optional condition.
	// The lockUntil value is always $1. Condition args start at $2.
	conditionClause := ""
	if s.condition != "" {
		conditionClause = fmt.Sprintf("AND %s", s.renumberCondition(2))
	}

	query := fmt.Sprintf(`
		WITH locked AS (
			SELECT id, sleep_until AS original_sleep_until
			FROM %s
			WHERE sleep_until IS NOT NULL AND sleep_until <= NOW()
			%s
			ORDER BY sleep_until ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s
		SET sleep_until = $1
		FROM locked
		WHERE %s.id = locked.id
		RETURNING %s.id, locked.original_sleep_until, %s.interval, %s.repeat_until, %s.auto_remove, %s.data`,
		s.tableName,
		conditionClause,
		s.tableName,
		s.tableName,
		s.tableName,
		s.tableName,
		s.tableName,
		s.tableName,
		s.tableName,
	)

	args := []interface{}{lockUntil}
	args = append(args, s.conditionArgs...)

	var (
		id          int64
		sleepUntil  *time.Time
		interval    string
		repeatUntil *time.Time
		autoRemove  bool
		dataBytes   []byte
	)

	err := s.pool.QueryRow(ctx, query, args...).Scan(
		&id, &sleepUntil, &interval, &repeatUntil, &autoRemove, &dataBytes,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("lock next failed: %w", err)
	}

	data := make(map[string]interface{})
	if len(dataBytes) > 0 {
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
	}

	job := &scheduler.Job{
		ID:          id,
		SleepUntil:  sleepUntil,
		Interval:    interval,
		RepeatUntil: repeatUntil,
		AutoRemove:  autoRemove,
		Data:        data,
	}

	return job, nil
}

// Update modifies a job's fields.
func (s *Store) Update(ctx context.Context, jobID interface{}, updates scheduler.JobUpdate) error {
	if updates.SleepUntil == nil {
		return nil
	}

	var query string
	var args []interface{}

	if *updates.SleepUntil == nil {
		// Set sleep_until to NULL
		query = fmt.Sprintf("UPDATE %s SET sleep_until = NULL WHERE id = $1", s.tableName)
		args = []interface{}{jobID}
	} else {
		// Set sleep_until to the given time
		query = fmt.Sprintf("UPDATE %s SET sleep_until = $1 WHERE id = $2", s.tableName)
		args = []interface{}{**updates.SleepUntil, jobID}
	}

	result, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("job not found: %v", jobID)
	}

	return nil
}

// Remove deletes a job from the store.
func (s *Store) Remove(ctx context.Context, jobID interface{}) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.tableName)

	result, err := s.pool.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("job not found: %v", jobID)
	}

	return nil
}

// renumberCondition shifts parameter numbers in the condition string.
// If the condition uses $1, $2, etc., they get shifted by the given offset.
// For example, with startAt=2, $1 becomes $2, $2 becomes $3, etc.
func (s *Store) renumberCondition(startAt int) string {
	if len(s.conditionArgs) == 0 {
		return s.condition
	}

	result := []byte(s.condition)
	// Replace $N with $(N+startAt-1), processing from highest to lowest
	// to avoid $1 being replaced before $10.
	for i := len(s.conditionArgs); i >= 1; i-- {
		old := fmt.Sprintf("$%d", i)
		new := fmt.Sprintf("$%d", i+startAt-1)
		result = replaceAll(result, old, new)
	}
	return string(result)
}

// replaceAll replaces all occurrences of old with new in b.
func replaceAll(b []byte, old, new string) []byte {
	oldBytes := []byte(old)
	newBytes := []byte(new)
	var out []byte
	for len(b) > 0 {
		idx := indexOf(b, oldBytes)
		if idx < 0 {
			out = append(out, b...)
			break
		}
		out = append(out, b[:idx]...)
		out = append(out, newBytes...)
		b = b[idx+len(oldBytes):]
	}
	return out
}

// indexOf returns the index of needle in haystack, or -1 if not found.
func indexOf(haystack, needle []byte) int {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
