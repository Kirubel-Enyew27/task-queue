package worker_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"task-queue/internal/queue"
	"task-queue/internal/store/sqlite"
	"task-queue/internal/task"
	"task-queue/internal/worker"
)

func TestPool_ReclaimsStaleProcessingTask(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tasks.db")

	store, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	q := queue.NewWithStore(0, store, discardLog)
	if err := q.Enqueue(&task.Task{ID: "stale-task", Payload: []byte(`{"job":"stale"}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := q.UpdateStatus("stale-task", task.StatusProcessing, ""); err != nil {
		t.Fatalf("mark processing: %v", err)
	}

	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer rawDB.Close()

	oldTime := time.Now().UTC().Add(-2 * time.Minute).UnixNano()
	if _, err := rawDB.Exec(`UPDATE tasks SET updated_at = ? WHERE id = ?`, oldTime, "stale-task"); err != nil {
		t.Fatalf("age task: %v", err)
	}

	var processed atomic.Int32
	handler := func(_ context.Context, _ *task.Task) error {
		processed.Add(1)
		return nil
	}

	pool := worker.NewPool(1, 10*time.Millisecond, store, handler, discardLog)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(3 * time.Second)
	for {
		got, err := q.Get("stale-task")
		if err == nil && got.Status == task.StatusCompleted {
			break
		}

		select {
		case <-deadline:
			t.Fatal("timeout waiting for stale task to be processed")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	if processed.Load() != 1 {
		t.Fatalf("processed = %d, want 1", processed.Load())
	}
}
