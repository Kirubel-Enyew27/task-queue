package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"task-queue/internal/api"
	"task-queue/internal/queue"
	"task-queue/internal/store/sqlite"
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

	q := queue.NewWithStore(0, dbStore, log)
	httpHandler := api.NewHandler(q, log)
	server := &http.Server{
		Addr:    serverAddress,
		Handler: httpHandler,
	}

	serverErrCh := make(chan error, 1)

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

	log.Info("shutdown complete")
}

func taskDBPath() string {
	if path := os.Getenv("TASK_QUEUE_DB_PATH"); path != "" {
		return path
	}
	return filepath.Join(".", "task_queue.db")
}
