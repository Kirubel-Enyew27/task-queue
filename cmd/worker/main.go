package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"task-queue/internal/store/sqlite"
	"task-queue/internal/task"
	"task-queue/internal/worker"
	"time"
)

const defaultWorkerConcurrency = 3
const defaultPollInterval = 250 * time.Millisecond
const defaultTaskTimeout = 5 * time.Second
const defaultRetryBaseDelay = 250 * time.Millisecond
const defaultRetryMaxDelay = 30 * time.Second
const defaultMaxRetries = 3
const defaultClaimTimeout = time.Minute

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	dbPath := taskDBPath()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		log.Error("failed to create database directory", "path", dbPath, "err", err)
		os.Exit(1)
	}

	dbStore, err := sqlite.Open(dbPath)
	if err != nil {
		log.Error("failed to open sqlite store", "path", dbPath, "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := dbStore.Close(); err != nil {
			log.Error("failed to close sqlite store", "err", err)
		}
	}()

	concurrency := envInt("WORKER_CONCURRENCY", defaultWorkerConcurrency)
	pollInterval := envDuration("WORKER_POLL_INTERVAL", defaultPollInterval)
	taskTimeout := envDuration("WORKER_TASK_TIMEOUT", defaultTaskTimeout)
	retryBaseDelay := envDuration("WORKER_RETRY_BASE_DELAY", defaultRetryBaseDelay)
	retryMaxDelay := envDuration("WORKER_RETRY_MAX_DELAY", defaultRetryMaxDelay)
	maxRetries := envInt("WORKER_MAX_RETRIES", defaultMaxRetries)
	claimTimeout := envDuration("WORKER_CLAIM_TIMEOUT", defaultClaimTimeout)

	handler := func(ctx context.Context, t *task.Task) error {
		select {
		case <-time.After(500 * time.Millisecond):
			log.Info("task handled", "id", t.ID, "payload", string(t.Payload))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	pool := worker.NewPool(concurrency, pollInterval, dbStore, handler, log)
	pool.SetTaskTimeout(taskTimeout)
	pool.SetRetryPolicy(maxRetries, retryBaseDelay, retryMaxDelay)
	pool.SetClaimTimeout(claimTimeout)
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(workerCtx)
	}()

	select {
	case sig := <-sigCh:
		log.Info("shutdown requested", "signal", sig.String())
	case <-done:
	}

	cancelWorkers()
	<-done
	log.Info("shutdown complete")
}

func taskDBPath() string {
	if path := os.Getenv("TASK_QUEUE_DB_PATH"); path != "" {
		return path
	}
	return filepath.Join(".", "task_queue.db")
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
