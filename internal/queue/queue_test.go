package queue_test

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	"task-queue/internal/queue"
	"task-queue/internal/task"
)

var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func newTask(id string) *task.Task {
	return &task.Task{ID: id, Payload: []byte(`{"test": true}`)}
}

func TestEnqueue_PersistsTask(t *testing.T) {
	q := queue.New(10, discardLog)
	tk := newTask("t1")

	if err := q.Enqueue(tk); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	got, err := q.Get("t1")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != task.StatusPending {
		t.Fatalf("expected status %q, got %q", task.StatusPending, got.Status)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("CreatedAt should be set")
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt should be set")
	}
}

func TestEnqueue_DuplicateReturnsError(t *testing.T) {
	q := queue.New(10, discardLog)
	if err := q.Enqueue(newTask("dup")); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	if err := q.Enqueue(newTask("dup")); err == nil {
		t.Fatal("expected dupilcate task error")
	}
}

func TestUpdateStatus(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t3"))

	if err := q.UpdateStatus("t3", task.StatusCompleted, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := q.Get("t3")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != task.StatusCompleted {
		t.Errorf("expected %q, got %q", task.StatusCompleted, got.Status)
	}
}

func TestGet_NotFound(t *testing.T) {
	q := queue.New(5, discardLog)
	_, err := q.Get("ghost")
	if !errors.Is(err, task.ErrNotFound) {
		t.Fatalf("expected task.ErrNotFound, got %v", err)
	}
}

func TestUpdateStatus_SetsError(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t4"))
	_ = q.UpdateStatus("t4", task.StatusFailed, "something went wrong")

	got, err := q.Get("t4")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Error != "something went wrong" {
		t.Errorf("expected error message to be stored, got %q", got.Error)
	}
}

func TestGet_ReturnsCopy(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t5"))

	got, err := q.Get("t5")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}

	got.Status = task.StatusFailed
	got.Payload[0] = '{'

	original, err := q.Get("t5")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if original.Status != task.StatusPending {
		t.Fatalf("expected original status to remain pending, got %q", original.Status)
	}
	if string(original.Payload) != `{"test": true}` {
		t.Fatalf("expected original payload to remain unchanged, got %s", original.Payload)
	}
}
