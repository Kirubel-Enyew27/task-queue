package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-queue/internal/queue"
	"task-queue/internal/task"
	"task-queue/internal/worker"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Graceful shutdown: cancel context on SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	const (
		queueCapacity = 100
		workerCount   = 3
	)

	q := queue.New(queueCapacity, log)

	// handler simulates real work: sleeps briefly then prints the payload.
	handler := func(ctx context.Context, t *task.Task) error {
		select {
		case <-time.After(500 * time.Millisecond):
			log.Info("task handled", "id", t.ID, "payload", string(t.Payload))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	pool := worker.NewPool(workerCount, q, handler, log)

	// Enqueue a handful of demo tasks before workers start consuming.
	for i := range 10 {
		t := &task.Task{
			ID:      fmt.Sprintf("task-%03d", i+1),
			Payload: []byte(fmt.Sprintf(`{"job":"demo","index":%d}`, i+1)),
		}
		if err := q.Enqueue(t); err != nil {
			log.Error("enqueue failed", "err", err)
		}
	}

	log.Info("starting worker pool", "workers", workerCount)
	pool.Start(ctx) // blocks until ctx cancelled and all workers exit
	log.Info("shutdown complete")
}
