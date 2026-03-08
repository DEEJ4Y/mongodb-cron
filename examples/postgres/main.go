package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/DEEJ4Y/mongodb-cron"
	"github.com/DEEJ4Y/mongodb-cron/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	// Connect to PostgreSQL
	pgURI := os.Getenv("POSTGRES_URI")
	if pgURI == "" {
		pgURI = "postgres://localhost:5432/myapp?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, pgURI)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Create jobs table if it doesn't exist
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS jobs (
			id BIGSERIAL PRIMARY KEY,
			sleep_until TIMESTAMPTZ,
			interval TEXT NOT NULL DEFAULT '',
			repeat_until TIMESTAMPTZ,
			auto_remove BOOLEAN NOT NULL DEFAULT FALSE,
			data JSONB DEFAULT '{}'
		)`
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Create index for performance
	createIndexSQL := `
		CREATE INDEX IF NOT EXISTS idx_jobs_sleep_until
		ON jobs (sleep_until)
		WHERE sleep_until IS NOT NULL`
	if _, err := pool.Exec(ctx, createIndexSQL); err != nil {
		log.Printf("Warning: Failed to create index: %v", err)
	}

	// Create PostgreSQL store
	store, err := postgres.NewStore(postgres.Config{
		Pool: pool,
		// Optional: use a custom table name
		// TableName: "my_jobs",
		// Optional: add filter condition
		// Condition: "data->>'type' = 'email'",
	})
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}

	// Create scheduler
	sched, err := scheduler.New(scheduler.Config{
		Store: store,
		OnDocument: func(ctx context.Context, job *scheduler.Job) error {
			fmt.Printf("Processing job: %v\n", job.Data)
			return nil
		},
		OnStart: func(ctx context.Context) error {
			fmt.Println("Scheduler started")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			fmt.Println("Scheduler stopped")
			return nil
		},
		OnIdle: func(ctx context.Context) error {
			fmt.Println("No jobs available, entering idle mode")
			return nil
		},
		OnError: func(ctx context.Context, err error) {
			fmt.Printf("Error: %v\n", err)
		},
		NextDelay:      1 * time.Second,
		ReprocessDelay: 1 * time.Second,
		IdleDelay:      10 * time.Second,
		LockDuration:   10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	// Insert sample jobs
	now := time.Now()
	jobs := []struct {
		sleepUntil  time.Time
		interval    string
		repeatUntil *time.Time
		autoRemove  bool
		data        string
	}{
		{now, "", nil, false, `{"name": "Job #1 - Immediate"}`},
		{now.Add(2 * time.Second), "", nil, false, `{"name": "Job #2 - Deferred 2s"}`},
		{now.Add(3 * time.Second), "", nil, false, `{"name": "Job #3 - Deferred 3s"}`},
		{now, "*/5 * * * * *", nil, false, `{"name": "Job #4 - Recurring every 5 seconds"}`},
		{now.Add(5 * time.Second), "", nil, true, `{"name": "Job #5 - Auto-remove"}`},
	}

	// Insert with repeat_until for Job #6
	repeatUntil := now.Add(10 * time.Second)
	_, err = pool.Exec(ctx,
		"INSERT INTO jobs (sleep_until, interval, repeat_until, data) VALUES ($1, $2, $3, $4)",
		now, "*/2 * * * * *", repeatUntil,
		`{"name": "Job #6 - Recurring with expiration"}`,
	)
	if err != nil {
		log.Fatalf("Failed to insert job #6: %v", err)
	}

	for _, j := range jobs {
		var query string
		var args []interface{}
		if j.autoRemove {
			query = "INSERT INTO jobs (sleep_until, interval, auto_remove, data) VALUES ($1, $2, $3, $4)"
			args = []interface{}{j.sleepUntil, j.interval, j.autoRemove, j.data}
		} else {
			query = "INSERT INTO jobs (sleep_until, interval, data) VALUES ($1, $2, $3)"
			args = []interface{}{j.sleepUntil, j.interval, j.data}
		}
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			log.Fatalf("Failed to insert job: %v", err)
		}
	}
	fmt.Printf("Inserted %d jobs\n", len(jobs)+1)

	// Start scheduler
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fmt.Println("\nShutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := sched.Stop(shutdownCtx); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
