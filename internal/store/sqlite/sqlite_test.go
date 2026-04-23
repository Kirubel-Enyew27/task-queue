package sqlite_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"task-queue/internal/store/sqlite"
	"task-queue/internal/task"
)

func TestStore_SaveGetUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.Open(filepath.Join(dir, "tasks.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	now := time.Now().UTC().Truncate(time.Microsecond)
	tk := &task.Task{
		ID:        "sqlite-task-1",
		Payload:   []byte(`{"job":"persist"}`),
		Status:    task.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := store.Save(tk); err != nil {
		t.Fatalf("save task: %v", err)
	}

	got, err := store.Get(tk.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.ID != tk.ID {
		t.Fatalf("id = %q, want %q", got.ID, tk.ID)
	}
	if string(got.Payload) != string(tk.Payload) {
		t.Fatalf("payload = %s, want %s", got.Payload, tk.Payload)
	}
	if got.Status != task.StatusPending {
		t.Fatalf("status = %q, want %q", got.Status, task.StatusPending)
	}

	if err := store.UpdateStatus(tk.ID, task.StatusCompleted, ""); err != nil {
		t.Fatalf("update status: %v", err)
	}

	updated, err := store.Get(tk.ID)
	if err != nil {
		t.Fatalf("get updated task: %v", err)
	}
	if updated.Status != task.StatusCompleted {
		t.Fatalf("status = %q, want %q", updated.Status, task.StatusCompleted)
	}
	if !updated.UpdatedAt.After(got.UpdatedAt) && !updated.UpdatedAt.Equal(got.UpdatedAt) {
		t.Fatalf("expected updated timestamp to be set")
	}

	if err := store.Delete(tk.ID); err != nil {
		t.Fatalf("delete task: %v", err)
	}

	if _, err := store.Get(tk.ID); !errors.Is(err, task.ErrNotFound) {
		t.Fatalf("expected task.ErrNotFound after delete, got %v", err)
	}
}
