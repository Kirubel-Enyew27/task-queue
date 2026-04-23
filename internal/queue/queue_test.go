package queue_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"task-queue/internal/queue"
	"task-queue/internal/task"
)

var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func newTask(id string) *task.Task {
	return &task.Task{ID: id, Payload: []byte(`{"test": true}`)}
}

func TestEnqueue_SetsStatusAndTimeStamps(t *testing.T) {
	q := queue.New(10, discardLog)
	tk := newTask("t1")

	if err := q.Enqueue(tk); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	got, ok := q.Get("t1")
	if !ok {
		t.Fatal("task not found after enqueue")
	}
	if got.Status != task.StatusPending {
		t.Errorf("expected status %q, got %q", task.StatusPending, got.Status)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set")
	}
}

func TestEnqueue_Full_ReturnsError(t *testing.T) {
	q := queue.New(1, discardLog)
	_ = q.Enqueue(newTask("t1"))

	err := q.Enqueue(newTask("t2"))
	if err == nil {
		t.Fatal("expected error on full queue, got nil")
	}

	if _, ok := q.Get("t2"); ok {
		t.Fatal("task should not be stored when enqueue fails")
	}
}

func TestEnqueue_AfterDrain_ReturnsError(t *testing.T) {
	q := queue.New(1, discardLog)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q.Drain(ctx)

	err := q.Enqueue(newTask("t3"))
	if err == nil {
		t.Fatal("expected error when enqueueing to closed queue")
	}
}

func TestDequeue_ReceivesEnqueuedTask(t *testing.T) {
	q := queue.New(5, discardLog)
	tk := newTask("t2")
	_ = q.Enqueue(tk)

	select {
	case got := <-q.Dequeue():
		if got.ID != "t2" {
			t.Errorf("expected task id t2, got %s", got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for task on channel")
	}
}

func TestUpdateStatus(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t3"))

	if err := q.UpdateStatus("t3", task.StatusCompleted, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := q.Get("t3")
	if got.Status != task.StatusCompleted {
		t.Errorf("expected %q, got %q", task.StatusCompleted, got.Status)
	}
}

func TestUpdateStatus_UnknownID_ReturnsError(t *testing.T) {
	q := queue.New(5, discardLog)
	err := q.UpdateStatus("nonexistent", task.StatusFailed, "boom")
	if err == nil {
		t.Fatal("expected error for unknown task id")
	}
}

func TestGet_NotFound(t *testing.T) {
	q := queue.New(5, discardLog)
	_, ok := q.Get("ghost")
	if ok {
		t.Error("expected ok=false or missing task")
	}
}

func TestUpdateStatus_SetsError(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t4"))
	_ = q.UpdateStatus("t4", task.StatusFailed, "something went wrong")

	got, _ := q.Get("t4")
	if got.Error != "something went wrong" {
		t.Errorf("expected error message to be stored, got %q", got.Error)
	}
}

func TestGet_ReturnsCopy(t *testing.T) {
	q := queue.New(5, discardLog)
	_ = q.Enqueue(newTask("t5"))

	got, ok := q.Get("t5")
	if !ok {
		t.Fatal("task not found")
	}

	got.Status = task.StatusFailed
	got.Payload[0] = '{'

	original, _ := q.Get("t5")
	if original.Status != task.StatusPending {
		t.Fatalf("expected original status to remain pending, got %q", original.Status)
	}
	if string(original.Payload) != `{"test": true}` {
		t.Fatalf("expected original payload to remain unchanged, got %s", original.Payload)
	}
}
