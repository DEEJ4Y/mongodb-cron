package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	scheduler "github.com/DEEJ4Y/mongodb-cron"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDistributedLocking validates that jobs execute exactly once under high concurrency.
// This test simulates 100 concurrent scheduler instances processing 10,000 jobs.
func TestDistributedLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	const (
		numSchedulers = 100
		numJobs       = 10000
		testTimeout   = 5 * time.Minute
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Connect to PostgreSQL
	pgURI := os.Getenv("POSTGRES_URI")
	if pgURI == "" {
		pgURI = "postgres://localhost:5432/scheduler_test?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, pgURI)
	if err != nil {
		t.Skipf("Skipping test: PostgreSQL not available: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping test: Cannot ping PostgreSQL: %v", err)
	}

	// Use a unique table for this test
	tableName := fmt.Sprintf("jobs_test_%d", time.Now().Unix())

	// Create table
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGSERIAL PRIMARY KEY,
			sleep_until TIMESTAMPTZ,
			interval TEXT NOT NULL DEFAULT '',
			repeat_until TIMESTAMPTZ,
			auto_remove BOOLEAN NOT NULL DEFAULT FALSE,
			data JSONB DEFAULT '{}'
		)`, tableName)
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create index
	createIndexSQL := fmt.Sprintf(
		"CREATE INDEX idx_%s_sleep_until ON %s (sleep_until) WHERE sleep_until IS NOT NULL",
		tableName, tableName)
	if _, err := pool.Exec(ctx, createIndexSQL); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	t.Logf("Test configuration: %d schedulers, %d jobs, timeout: %v", numSchedulers, numJobs, testTimeout)

	// Job execution tracking
	executions := &ExecutionTracker{
		counts:     make(map[string]int),
		timestamps: make(map[string][]time.Time),
	}

	// Insert jobs
	t.Log("Inserting jobs...")
	startInsert := time.Now()
	if err := insertJobs(ctx, pool, tableName, numJobs); err != nil {
		t.Fatalf("Failed to insert jobs: %v", err)
	}
	t.Logf("Inserted %d jobs in %v", numJobs, time.Since(startInsert))

	// Start schedulers
	t.Logf("Starting %d concurrent schedulers...", numSchedulers)
	startTime := time.Now()

	var (
		schedulers []*scheduler.Scheduler
		wg         sync.WaitGroup
		errorCount atomic.Int64
		startMutex sync.Mutex
		stopping   atomic.Bool
	)

	// Create and start all schedulers
	for i := 0; i < numSchedulers; i++ {
		schedulerID := i

		store, err := NewStore(Config{
			Pool:      pool,
			TableName: tableName,
		})
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}

		sched, err := scheduler.New(scheduler.Config{
			Store: store,
			OnDocument: func(ctx context.Context, job *scheduler.Job) error {
				jobID := getJobID(job)
				executions.Record(jobID)
				return nil
			},
			OnError: func(ctx context.Context, err error) {
				if stopping.Load() && strings.Contains(err.Error(), "context canceled") {
					return
				}
				errorCount.Add(1)
				t.Logf("Scheduler %d error: %v", schedulerID, err)
			},
			NextDelay:    10 * time.Millisecond,
			LockDuration: 30 * time.Second,
		})
		if err != nil {
			t.Fatalf("Failed to create scheduler: %v", err)
		}

		schedulers = append(schedulers, sched)
	}

	// Start all schedulers simultaneously for maximum concurrency
	startMutex.Lock()
	for i, sched := range schedulers {
		wg.Add(1)
		go func(idx int, s *scheduler.Scheduler) {
			defer wg.Done()

			startMutex.Lock()
			startMutex.Unlock()

			if err := s.Start(ctx); err != nil {
				t.Errorf("Scheduler %d failed to start: %v", idx, err)
			}
		}(i, sched)
	}

	// Release all schedulers at once
	time.Sleep(100 * time.Millisecond)
	startMutex.Unlock()

	// Monitor progress
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()

	go func() {
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				completed := executions.TotalExecutions()
				remaining := countRemainingJobs(context.Background(), pool, tableName)
				t.Logf("Progress: %d/%d jobs executed, %d remaining, %d errors",
					completed, numJobs, remaining, errorCount.Load())
			}
		}
	}()

	// Wait for all jobs to be processed
	allProcessed := false
	checkInterval := 2 * time.Second
	maxIdleTime := 10 * time.Second
	lastProgress := executions.TotalExecutions()
	idleStart := time.Now()

	for !allProcessed {
		select {
		case <-ctx.Done():
			t.Fatal("Test timeout reached")
		case <-time.After(checkInterval):
			completed := executions.TotalExecutions()
			remaining := countRemainingJobs(context.Background(), pool, tableName)

			if completed != lastProgress {
				lastProgress = completed
				idleStart = time.Now()
			}

			if remaining == 0 && completed >= numJobs {
				allProcessed = true
			} else if time.Since(idleStart) > maxIdleTime && remaining == 0 {
				allProcessed = true
			}
		}
	}

	monitorCancel()
	duration := time.Since(startTime)

	// Stop all schedulers
	t.Log("Stopping schedulers...")
	stopping.Store(true)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()

	for i, sched := range schedulers {
		if err := sched.Stop(stopCtx); err != nil {
			t.Logf("Warning: Scheduler %d stop error: %v", i, err)
		}
	}

	wg.Wait()

	// Analyze results
	separator := strings.Repeat("=", 80)
	t.Log("\n" + separator)
	t.Log("TEST RESULTS")
	t.Log(separator)

	stats := executions.GetStats()

	t.Logf("\nExecution Statistics:")
	t.Logf("  Total jobs queued:        %d", numJobs)
	t.Logf("  Total executions:         %d", stats.TotalExecutions)
	t.Logf("  Unique jobs executed:     %d", stats.UniqueJobs)
	t.Logf("  Jobs with duplicates:     %d", stats.DuplicateJobs)
	t.Logf("  Total duplicate runs:     %d", stats.TotalDuplicates)
	t.Logf("  Jobs not executed:        %d", stats.MissedJobs)
	t.Logf("  Errors encountered:       %d", errorCount.Load())

	t.Logf("\nPerformance Metrics:")
	t.Logf("  Total duration:           %v", duration)
	t.Logf("  Jobs per second:          %.2f", float64(stats.TotalExecutions)/duration.Seconds())
	if stats.TotalExecutions > 0 {
		t.Logf("  Avg time per job:         %v", duration/time.Duration(stats.TotalExecutions))
	}

	if len(stats.Duplicates) > 0 {
		t.Logf("\nDuplicate Executions Detected:")
		count := 0
		for jobID, execCount := range stats.Duplicates {
			if count < 10 {
				times := executions.GetTimestamps(jobID)
				t.Logf("  Job %s: executed %d times", jobID, execCount)
				for i, ts := range times {
					t.Logf("    Execution %d: %v", i+1, ts.Format(time.RFC3339Nano))
				}
			}
			count++
		}
		if count > 10 {
			t.Logf("  ... and %d more jobs with duplicates", count-10)
		}
	}

	if len(stats.MissedJobIDs) > 0 {
		t.Logf("\nMissed Jobs:")
		for i, jobID := range stats.MissedJobIDs {
			if i < 10 {
				t.Logf("  %s", jobID)
			}
		}
		if len(stats.MissedJobIDs) > 10 {
			t.Logf("  ... and %d more missed jobs", len(stats.MissedJobIDs)-10)
		}
	}

	t.Log(separator)

	// Validate results
	if stats.TotalDuplicates > 0 {
		t.Errorf("FAILED: Found %d duplicate executions across %d jobs",
			stats.TotalDuplicates, stats.DuplicateJobs)
	}

	if stats.MissedJobs > 0 {
		t.Errorf("FAILED: %d jobs were not executed", stats.MissedJobs)
	}

	if stats.UniqueJobs != numJobs {
		t.Errorf("FAILED: Expected %d unique jobs executed, got %d",
			numJobs, stats.UniqueJobs)
	}

	if stats.TotalDuplicates == 0 && stats.MissedJobs == 0 && stats.UniqueJobs == numJobs {
		t.Logf("\n✓ SUCCESS: All %d jobs executed exactly once with no duplicates!", numJobs)
		t.Logf("✓ Distributed locking mechanism is working correctly under high concurrency")
	}
}

// ExecutionTracker tracks job executions in a thread-safe manner.
type ExecutionTracker struct {
	mu         sync.RWMutex
	counts     map[string]int
	timestamps map[string][]time.Time
}

func (et *ExecutionTracker) Record(jobID string) {
	et.mu.Lock()
	defer et.mu.Unlock()

	et.counts[jobID]++
	et.timestamps[jobID] = append(et.timestamps[jobID], time.Now())
}

func (et *ExecutionTracker) TotalExecutions() int {
	et.mu.RLock()
	defer et.mu.RUnlock()

	total := 0
	for _, count := range et.counts {
		total += count
	}
	return total
}

func (et *ExecutionTracker) GetTimestamps(jobID string) []time.Time {
	et.mu.RLock()
	defer et.mu.RUnlock()

	return et.timestamps[jobID]
}

type ExecutionStats struct {
	TotalExecutions int
	UniqueJobs      int
	DuplicateJobs   int
	TotalDuplicates int
	MissedJobs      int
	Duplicates      map[string]int
	MissedJobIDs    []string
}

func (et *ExecutionTracker) GetStats() ExecutionStats {
	et.mu.RLock()
	defer et.mu.RUnlock()

	stats := ExecutionStats{
		Duplicates:   make(map[string]int),
		MissedJobIDs: make([]string, 0),
	}

	stats.UniqueJobs = len(et.counts)

	for jobID, count := range et.counts {
		stats.TotalExecutions += count

		if count > 1 {
			stats.DuplicateJobs++
			stats.TotalDuplicates += (count - 1)
			stats.Duplicates[jobID] = count
		}
	}

	return stats
}

// Helper functions

func insertJobs(ctx context.Context, pool *pgxpool.Pool, tableName string, count int) error {
	now := time.Now()

	batchSize := 1000
	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}

		query := fmt.Sprintf("INSERT INTO %s (sleep_until, data) VALUES ", tableName)
		args := make([]interface{}, 0, (end-i)*2)
		values := make([]string, 0, end-i)

		for j := i; j < end; j++ {
			paramIdx := (j - i) * 2
			values = append(values, fmt.Sprintf("($%d, $%d)", paramIdx+1, paramIdx+2))
			args = append(args, now, fmt.Sprintf(`{"jobID": "job-%06d", "data": "test-job-%d"}`, j, j))
		}

		query += strings.Join(values, ", ")

		if _, err := pool.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("batch insert failed: %w", err)
		}
	}

	return nil
}

func countRemainingJobs(ctx context.Context, pool *pgxpool.Pool, tableName string) int64 {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sleep_until IS NOT NULL", tableName)
	pool.QueryRow(ctx, query).Scan(&count)
	return count
}

func getJobID(job *scheduler.Job) string {
	if jobID, ok := job.Data["jobID"].(string); ok {
		return jobID
	}
	return fmt.Sprintf("%v", job.ID)
}
