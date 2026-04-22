package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-queue/internal/api"
	"task-queue/internal/queue"
	"task-queue/internal/task"
	"task-queue/internal/worker"
)

const shutdownTimeout = 5 * time.Second
const serverAddress = ":8080"

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
	httpHandler := api.NewHandler(q, log)
	server := &http.Server{
		Addr:    serverAddress,
		Handler: httpHandler,
	}

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

	serverErrCh := make(chan error, 1)

	log.Info("starting worker pool", "workers", workerCount)
	go func() {
		defer close(poolDone)
		pool.Start(workerCtx)
	}()

	go func() {
		log.Info("http server listening", "addr", serverAddress)
		serverErrCh <- server.ListenAndServe()
	}()

	select {
	case sig := <-sigCh:
		log.Info("shutdown requested", "signal", sig.String())
	case err := <-serverErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped unexpectedly", "err", err)
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("http server shutdown failed", "err", err)
	}

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
