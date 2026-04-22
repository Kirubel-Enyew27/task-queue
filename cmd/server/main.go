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

const shutdownTimeout = 5 * time.Second

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

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
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	poolDone := make(chan struct{})

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
	go func() {
		defer close(poolDone)
		pool.Start(workerCtx)
	}()

	sig := <-sigCh
	log.Info("shutdown requested", "signal", sig.String())
	closeQueue(q)
	waitForPoolShutdown(poolDone, cancelWorkers, shutdownTimeout, log)
	log.Info("shutdown complete")
}

func closeQueue(q *queue.Queue) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q.Drain(ctx)
}

func waitForPoolShutdown(done <-chan struct{}, cancel context.CancelFunc, timeout time.Duration, log *slog.Logger) {
	select {
	case <-done:
		return
	case <-time.After(timeout):
		log.Warn("shutdown timed out, canceling workers")
		cancel()
		<-done
	}
}
