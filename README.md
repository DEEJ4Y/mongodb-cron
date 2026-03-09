# Go Scheduler

> Database-agnostic job scheduler for Go, inspired by [mongodb-cron](https://github.com/xpepermint/mongodb-cron)

A flexible, database-agnostic job scheduling library for Go that can turn any database into a job queue or crontab-like system. Built with a clean architecture that separates core scheduling logic from database-specific implementations.

## Features

- **Database Agnostic**: Clean interface allows any database to be used (MongoDB, PostgreSQL, Redis, etc.)
- **Multiple Job Types**:
  - One-time jobs
  - Deferred jobs (schedule for future execution)
  - Recurring jobs (cron expressions)
  - Auto-removable jobs
- **Distributed Safe**: Uses atomic locking to prevent race conditions
- **Crash Recovery**: Jobs locked by crashed workers automatically become available
- **Event-Driven**: Hooks for job processing, errors, idle states, etc.
- **Concurrent-Safe**: Built with Go concurrency primitives
- **Context Support**: Proper context handling for cancellation and timeouts

## Installation

```bash
go get github.com/DEEJ4Y/mongodb-cron
```

For MongoDB support:
```bash
go get github.com/DEEJ4Y/mongodb-cron/mongodb
```

For PostgreSQL support:
```bash
go get github.com/DEEJ4Y/mongodb-cron/postgres
```

## Quick Start

### Basic Example (In-Memory)

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/DEEJ4Y/mongodb-cron"
)

func main() {
    ctx := context.Background()

    // Create a scheduler with an in-memory store
    // (see examples/basic for full implementation)
    store := NewInMemoryStore() // your implementation

    sched, err := scheduler.New(scheduler.Config{
        Store: store,
        OnDocument: func(ctx context.Context, job *scheduler.Job) error {
            fmt.Printf("Processing job: %v\n", job.Data)
            return nil
        },
        NextDelay:      1 * time.Second,
        LockDuration:   10 * time.Minute,
    })
    if err != nil {
        panic(err)
    }

    // Start processing
    sched.Start(ctx)
    defer sched.Stop(context.Background())

    // Add jobs to your store...
}
```

### MongoDB Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/DEEJ4Y/mongodb-cron"
    "github.com/DEEJ4Y/mongodb-cron/mongodb"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
    ctx := context.Background()

    // Connect to MongoDB
    client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(ctx)

    collection := client.Database("myapp").Collection("jobs")

    // Create MongoDB store
    store, err := mongodb.NewStore(mongodb.Config{
        Collection: collection,
    })
    if err != nil {
        panic(err)
    }

    // Create scheduler
    sched, err := scheduler.New(scheduler.Config{
        Store: store,
        OnDocument: func(ctx context.Context, job *scheduler.Job) error {
            fmt.Printf("Processing: %v\n", job.Data["name"])
            // Do your work here
            return nil
        },
        OnError: func(ctx context.Context, err error) {
            fmt.Printf("Error: %v\n", err)
        },
        NextDelay:      1 * time.Second,
        IdleDelay:      10 * time.Second,
        LockDuration:   10 * time.Minute,
    })
    if err != nil {
        panic(err)
    }

    // Start scheduler
    sched.Start(ctx)
    defer sched.Stop(context.Background())

    // Create jobs
    now := time.Now()
    collection.InsertOne(ctx, bson.M{
        "name":       "My Job",
        "sleepUntil": now,
        "data":       "custom payload",
    })
}
```

### PostgreSQL Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/DEEJ4Y/mongodb-cron"
    "github.com/DEEJ4Y/mongodb-cron/postgres"
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()

    // Connect to PostgreSQL
    pool, err := pgxpool.New(ctx, "postgres://localhost:5432/myapp?sslmode=disable")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    // Create table (run once)
    pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS jobs (
            id BIGSERIAL PRIMARY KEY,
            sleep_until TIMESTAMPTZ,
            interval TEXT NOT NULL DEFAULT '',
            repeat_until TIMESTAMPTZ,
            auto_remove BOOLEAN NOT NULL DEFAULT FALSE,
            data JSONB DEFAULT '{}'
        )`)

    // Create PostgreSQL store
    store, err := postgres.NewStore(postgres.Config{
        Pool: pool,
    })
    if err != nil {
        panic(err)
    }

    // Create scheduler
    sched, err := scheduler.New(scheduler.Config{
        Store: store,
        OnDocument: func(ctx context.Context, job *scheduler.Job) error {
            fmt.Printf("Processing: %v\n", job.Data["name"])
            return nil
        },
        OnError: func(ctx context.Context, err error) {
            fmt.Printf("Error: %v\n", err)
        },
        NextDelay:    1 * time.Second,
        IdleDelay:    10 * time.Second,
        LockDuration: 10 * time.Minute,
    })
    if err != nil {
        panic(err)
    }

    // Start scheduler
    sched.Start(ctx)
    defer sched.Stop(context.Background())

    // Create jobs
    now := time.Now()
    pool.Exec(ctx,
        "INSERT INTO jobs (sleep_until, data) VALUES ($1, $2)",
        now, `{"name": "My Job", "payload": "custom data"}`,
    )
}
```

## Job Types

### One-Time Job

Execute once immediately or at a specific time:

```go
// Immediate execution
collection.InsertOne(ctx, bson.M{
    "sleepUntil": time.Now(),
    "data": "my job data",
})

// Deferred execution
collection.InsertOne(ctx, bson.M{
    "sleepUntil": time.Now().Add(1 * time.Hour),
    "data": "process later",
})
```

### Recurring Job

Execute repeatedly based on a cron expression:

```go
collection.InsertOne(ctx, bson.M{
    "sleepUntil": time.Now(),
    "interval":   "*/5 * * * * *", // every 5 seconds
    "data":       "recurring task",
})
```

Cron format (6 fields):
```
* * * * * *
┬ ┬ ┬ ┬ ┬ ┬
│ │ │ │ │ └── day of week (0 - 7) (0 or 7 is Sunday)
│ │ │ │ └──── month (1 - 12)
│ │ │ └────── day of month (1 - 31)
│ │ └──────── hour (0 - 23)
│ └────────── minute (0 - 59)
└──────────── second (0 - 59)
```

### Recurring Job with Expiration

Stop repeating after a certain time:

```go
collection.InsertOne(ctx, bson.M{
    "sleepUntil":  time.Now(),
    "interval":    "0 0 * * * *", // every hour
    "repeatUntil": time.Now().Add(24 * time.Hour), // stop after 24 hours
})
```

### Auto-Remove Job

Automatically delete after completion:

```go
collection.InsertOne(ctx, bson.M{
    "sleepUntil": time.Now(),
    "autoRemove": true,
})
```

## Configuration

### Scheduler Config

```go
type Config struct {
    // Required
    Store JobStore  // Database implementation

    // Event Handlers (all optional)
    OnDocument func(ctx context.Context, job *Job) error  // Job processing
    OnStart    func(ctx context.Context) error            // Scheduler start
    OnStop     func(ctx context.Context) error            // Scheduler stop
    OnIdle     func(ctx context.Context) error            // No jobs available
    OnError    func(ctx context.Context, err error)       // Error occurred

    // Timing Configuration
    NextDelay      time.Duration  // Wait before next job (default: 0)
    ReprocessDelay time.Duration  // Wait before reprocessing recurring job (default: 0)
    IdleDelay      time.Duration  // Wait when no jobs available (default: 0)
    LockDuration   time.Duration  // How long jobs are locked (default: 10 minutes)
}
```

### MongoDB Config

```go
type Config struct {
    Collection *mongo.Collection  // Required

    // Custom field names (optional)
    SleepUntilField  string  // default: "sleepUntil"
    IntervalField    string  // default: "interval"
    RepeatUntilField string  // default: "repeatUntil"
    AutoRemoveField  string  // default: "autoRemove"

    // Additional filter condition (optional)
    Condition bson.M  // e.g., bson.M{"type": "email"}
}
```

### PostgreSQL Config

```go
type Config struct {
    Pool      *pgxpool.Pool   // Required
    TableName string          // default: "jobs"
    Condition string          // Optional WHERE clause, e.g. "data->>'type' = 'email'"
    ConditionArgs []interface{} // Args for condition placeholders
}
```

## Architecture

### Core Components

1. **JobStore Interface**: Abstracts database operations
   - `LockNext()`: Atomically locks the next available job
   - `Update()`: Updates job fields
   - `Remove()`: Deletes a job

2. **Scheduler**: Core scheduling logic (database-agnostic)
   - Polling loop for job processing
   - Event-driven hooks
   - Graceful shutdown

3. **Database Implementations**:
   - MongoDB (included)
   - PostgreSQL (included)
   - Redis, etc. (can be implemented by users)

### How It Works

1. Scheduler continuously polls for available jobs
2. Jobs with `sleepUntil <= now` are candidates for processing
3. `LockNext()` atomically locks a job by updating `sleepUntil` to `now + lockDuration`
4. Job is processed by calling `OnDocument` handler
5. After processing:
   - One-time jobs: `sleepUntil` set to `nil` (completed)
   - Recurring jobs: `sleepUntil` set to next execution time
   - Auto-remove jobs: deleted from database

### Crash Recovery

If a worker crashes while processing a job:
- Job remains locked until `lockDuration` expires
- After expiration, job becomes available for another worker
- Ensures jobs are eventually processed even after crashes

## Implementing Your Own Store

Implement the `JobStore` interface for your database:

```go
type JobStore interface {
    LockNext(ctx context.Context, lockUntil time.Time) (*Job, error)
    Update(ctx context.Context, jobID interface{}, updates JobUpdate) error
    Remove(ctx context.Context, jobID interface{}) error
}
```

See `mongodb/store.go` for a complete example.

Key requirements:
- `LockNext` must be atomic to prevent race conditions
- Return the job data BEFORE locking (original `sleepUntil` value)
- Handle `nil` values correctly for `sleepUntil`

## Performance

### PostgreSQL Indexes

For better performance with PostgreSQL, create a partial index on `sleep_until`:

```sql
CREATE INDEX idx_jobs_sleep_until ON jobs (sleep_until) WHERE sleep_until IS NOT NULL;
```

### MongoDB Indexes

For better performance with MongoDB, create an index on `sleepUntil`:

```go
indexModel := mongo.IndexModel{
    Keys: bson.D{{Key: "sleepUntil", Value: 1}},
    Options: options.Index().SetSparse(true),
}
collection.Indexes().CreateOne(ctx, indexModel)
```

Adjust the index if using custom field paths or conditions.

### Tuning

- **NextDelay**: Reduce CPU usage by adding delay between job checks
- **IdleDelay**: When no jobs are available, wait longer before checking again
- **LockDuration**: Balance between quick recovery and avoiding duplicate processing
- **ReprocessDelay**: For recurring jobs, add delay before next execution

## Best Practices

1. **Idempotent Jobs**: Design jobs to be safely re-executed
   - A crashed worker may process a job twice
   - Use unique transaction IDs or database constraints

2. **Graceful Shutdown**: Always call `Stop()` with a timeout
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
   defer cancel()
   sched.Stop(ctx)
   ```

3. **Error Handling**: Use `OnError` handler for observability
   ```go
   OnError: func(ctx context.Context, err error) {
       log.Printf("Scheduler error: %v", err)
       // Send to error tracking service
   }
   ```

4. **Horizontal Scaling**: Run multiple scheduler instances
   - Atomic locking prevents duplicate processing
   - Jobs are distributed across workers

5. **Monitoring**: Track scheduler state
   ```go
   if sched.IsIdle() {
       // No jobs in queue
   }
   if sched.IsProcessing() {
       // Currently processing a job
   }
   ```

## Examples

See the `examples/` directory:
- `examples/basic/`: In-memory implementation for testing
- `examples/mongodb/`: Production-ready MongoDB example
- `examples/postgres/`: Production-ready PostgreSQL example

## Testing

Run tests:
```bash
go test ./...
```

Run with coverage:
```bash
go test -cover ./...
```

### Concurrency Tests

Comprehensive distributed locking tests are included to validate exactly-once execution under high concurrency:

```bash
# In-memory test (50 schedulers, 5,000 jobs)
go test -v -run TestConcurrentSchedulers .

# Large stress test (100 schedulers, 10,000 jobs)
go test -v -run TestConcurrentSchedulersLarge .

# MongoDB test (requires MongoDB running)
go test -v -run TestDistributedLocking ./mongodb/

# PostgreSQL test (requires PostgreSQL running)
go test -v -run TestDistributedLocking ./postgres/
```

## Concurrency Testing & Performance

The scheduler has been extensively tested for distributed locking correctness and performance. See [CONCURRENCY_TEST_RESULTS.md](CONCURRENCY_TEST_RESULTS.md) for full details.

### Test Results Summary

**✅ All tests passed with ZERO duplicate executions**

| Test | Schedulers | Jobs | Duration | Throughput | Duplicates | Errors |
|------|------------|------|----------|------------|------------|--------|
| In-Memory (Go) | 50 | 5,000 | 1.1s | 4,539 jobs/sec | 0 ✅ | 0 ✅ |
| Stress Test (Go) | 100 | 10,000 | 2.2s | 4,519 jobs/sec | 0 ✅ | 0 ✅ |
| **MongoDB (Go)** | **100** | **10,000** | **2.1s** | **4,757 jobs/sec** | **0 ✅** | **0 ✅** |
| **PostgreSQL (Go)** | **100** | **10,000** | **8.2s** | **1,224 jobs/sec** | **0 ✅** | **0 ✅** |
| **MongoDB (Node.js)** | **100** | **10,000** | **4.0s** | **2,492 jobs/sec** | **0 ✅** | **0 ✅** |

### Go vs Node.js Performance Comparison

Direct comparison with identical test parameters (100 workers, 10,000 jobs, MongoDB):

| Metric | Go | Node.js | Difference |
|--------|-------|---------|------------|
| **Throughput** | 4,757 jobs/sec | 2,492 jobs/sec | **Go 1.91x faster** |
| **Duration** | 2.1s | 4.0s | **Go 1.91x faster** |
| **Latency** | 210µs/job | 401µs/job | **Go 1.91x lower** |
| **Correctness** | 0 duplicates ✅ | 0 duplicates ✅ | **Both perfect** |

**Key Takeaway**: The Go implementation delivers **~2x better performance** while maintaining the same perfect correctness as the original Node.js implementation. Both achieve zero duplicate executions under extreme concurrency (100 workers).

### Key Validation Points

✅ **Zero Duplicate Executions**: Across 35,000+ total job executions, not a single duplicate was found
✅ **Zero Missed Jobs**: Every queued job was processed exactly once
✅ **Production Databases**: Real MongoDB and PostgreSQL with 100 concurrent workers proves distributed safety
✅ **High Throughput**: 1,200-4,700 jobs/second depending on database backend
✅ **Race Condition Testing**: All schedulers started simultaneously to maximize contention

### What Was Tested

The concurrency tests validate the distributed locking mechanism under worst-case scenarios:

- **100 concurrent scheduler instances** competing for the same jobs
- **Simultaneous start** of all schedulers to maximize race conditions
- **Fast polling** (2-10ms intervals) to create maximum lock contention
- **Atomic operations** using MongoDB's `findOneAndUpdate` and PostgreSQL's `UPDATE ... RETURNING`
- **Crash recovery** scenarios with lock expiration

### Performance Characteristics

**MongoDB:**
```
MongoDB Distributed Test Results:
  Total jobs queued:        10,000
  Total executions:         10,000
  Unique jobs executed:     10,000
  Jobs with duplicates:     0
  Total duplicate runs:     0
  Jobs not executed:        0
  Errors encountered:       0

Performance Metrics:
  Jobs per second:          4,757.47
  Avg time per job:         210.195µs
  Throughput per scheduler: 47.57 jobs/sec
```

**PostgreSQL:**
```
PostgreSQL Distributed Test Results:
  Total jobs queued:        10,000
  Total executions:         10,000
  Unique jobs executed:     10,000
  Jobs with duplicates:     0
  Total duplicate runs:     0
  Jobs not executed:        0
  Errors encountered:       0

Performance Metrics:
  Jobs per second:          1,223.84
  Avg time per job:         817.102µs
  Throughput per scheduler: 12.24 jobs/sec
```

> **Note:** PostgreSQL throughput is lower due to its stronger transactional guarantees and row-level locking semantics. Both databases achieve **zero duplicate executions**, proving the distributed locking mechanism works correctly regardless of backend.

This proves the scheduler is **production-ready** for:
- ✅ Distributed deployments with multiple workers
- ✅ High-throughput job processing
- ✅ Mission-critical applications requiring exactly-once execution
- ✅ Horizontal scaling scenarios

## Differences from mongodb-cron

This Go implementation maintains feature parity with the Node.js mongodb-cron package while offering significant improvements:

### Performance Benefits

- **~2x faster throughput**: 4,757 vs 2,492 jobs/sec (tested with 100 workers, 10K jobs)
- **~2x lower latency**: 210µs vs 401µs per job
- **Half the execution time**: Completes workloads in ~50% less time
- **Same correctness**: Both achieve zero duplicate executions ✅

### Architectural Improvements

- **Database abstraction**: Not limited to MongoDB - supports any database via JobStore interface
- **Go idioms**: Context support, proper error handling, concurrent-safe primitives
- **Type safety**: Strongly typed API prevents runtime errors
- **Modern Go**: Uses Go modules, goroutines, latest best practices
- **Compiled performance**: Native binary offers better resource utilization

### When to Choose Go Over Node.js

**Choose this Go implementation** if you need:
- ✅ Maximum performance (>2,000 jobs/sec)
- ✅ Low latency requirements (<300µs per job)
- ✅ Database flexibility (PostgreSQL, Redis, etc.)
- ✅ Efficient resource usage in cloud environments
- ✅ Strong typing and compile-time safety

**Choose Node.js mongodb-cron** if you:
- ✅ Have an existing Node.js/TypeScript codebase
- ✅ Need moderate throughput (<2,000 jobs/sec)
- ✅ Prefer JavaScript ecosystem and tooling
- ✅ Want simpler deployment (no compilation)

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or PR.

Areas for contribution:
- Additional database implementations (PostgreSQL, Redis, etc.)
- Performance improvements
- More comprehensive tests
- Documentation improvements

## Credits

Inspired by [mongodb-cron](https://github.com/xpepermint/mongodb-cron) by Kristijan Sedlak.
