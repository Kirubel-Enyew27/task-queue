package queue_test

import (
	"path/filepath"
	"testing"

	"task-queue/internal/queue"
	"task-queue/internal/store/sqlite"
	"task-queue/internal/task"
)

func TestQueue_WithSQLiteStore_PersistsState(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	q := queue.NewWithStore(5, store, discardLog)

	tk := newTask("sqlite-q-1")
	if err := q.Enqueue(tk); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Get("sqlite-q-1")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != task.StatusPending {
		t.Fatalf("status = %q, want %q", got.Status, task.StatusPending)
	}

	if err := q.UpdateStatus("sqlite-q-1", task.StatusCompleted, ""); err != nil {
		t.Fatalf("update status: %v", err)
	}

	updated, err := q.Get("sqlite-q-1")
	if err != nil {
		t.Fatalf("get updated task: %v", err)
	}
	if updated.Status != task.StatusCompleted {
		t.Fatalf("status = %q, want %q", updated.Status, task.StatusCompleted)
	}
}
