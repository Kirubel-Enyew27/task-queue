package memory_test

import (
	"testing"
	"time"

	"task-queue/internal/store/memory"
	"task-queue/internal/task"
)

func TestClaimAvailable_ClaimsPendingTask(t *testing.T) {
	store := memory.New()
	tk := &task.Task{ID: "task-1", Payload: []byte(`{"job":"demo"}`), Status: task.StatusPending}
	if err := store.Save(tk); err != nil {
		t.Fatalf("save task: %v", err)
	}

	claimed, err := store.ClaimAvailable(tk.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if !claimed {
		t.Fatal("expected task to be claimed")
	}

	got, err := store.Get(tk.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != task.StatusProcessing {
		t.Fatalf("status = %q, want %q", got.Status, task.StatusProcessing)
	}
}
