package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"task-queue/internal/task"
	"task-queue/internal/worker"
)

var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// fakeQueue implements worker.StatusUpdater for testing without a real Queue.
type fakeQueue struct {
	mu      sync.Mutex
	tasks   map[string]*task.Task
	ch      chan *task.Task
	updates []statusUpdate
}

type statusUpdate struct {
	id     string
	status task.Status
	errMsg string
}

func newFakeQueue(cap int) *fakeQueue {
	return &fakeQueue{
		tasks: make(map[string]*task.Task),
		ch:    make(chan *task.Task, cap),
	}
}

func (f *fakeQueue) Dequeue() <-chan *task.Task { return f.ch }

func (f *fakeQueue) UpdateStatus(id string, status task.Status, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, statusUpdate{id, status, errMsg})
	if t, ok := f.tasks[id]; ok {
		t.Status = status
	}
	return nil
}

func (f *fakeQueue) push(t *task.Task) {
	f.mu.Lock()
	f.tasks[t.ID] = t
	f.mu.Unlock()
	f.ch <- t
}

func (f *fakeQueue) close() { close(f.ch) }

// ---- Tests ----

func TestPool_ProcessesTasksSuccessfully(t *testing.T) {
	fq := newFakeQueue(10)
	var processed []string
	var mu sync.Mutex

	handler := func(_ context.Context, tk *task.Task) error {
		mu.Lock()
		processed = append(processed, tk.ID)
		mu.Unlock()
		return nil
	}

	pool := worker.NewPool(2, fq, handler, discardLog)

	fq.push(&task.Task{ID: "w1"})
	fq.push(&task.Task{ID: "w2"})
	fq.close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool.Start(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 2 {
		t.Errorf("expected 2 processed tasks, got %d", len(processed))
	}
}

func TestPool_MarksFailed_OnHandlerError(t *testing.T) {
	fq := newFakeQueue(5)

	handler := func(_ context.Context, _ *task.Task) error {
		return errors.New("simulated failure")
	}

	pool := worker.NewPool(1, fq, handler, discardLog)
	fq.push(&task.Task{ID: "bad-task"})
	fq.close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool.Start(ctx)

	fq.mu.Lock()
	defer fq.mu.Unlock()

	var finalStatus task.Status
	for _, u := range fq.updates {
		if u.id == "bad-task" {
			finalStatus = u.status
		}
	}
	if finalStatus != task.StatusFailed {
		t.Errorf("expected StatusFailed, got %q", finalStatus)
	}
}

func TestPool_StopsOnContextCancel(t *testing.T) {
	fq := newFakeQueue(10)

	handler := func(ctx context.Context, _ *task.Task) error {
		<-ctx.Done() // block until cancelled
		return ctx.Err()
	}

	pool := worker.NewPool(2, fq, handler, discardLog)

	// push one task that will block
	fq.push(&task.Task{ID: "blocking"})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		pool.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
		// pool exited cleanly after context cancel — pass
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not stop after context cancellation")
	}
}

func TestPool_SetsProcessingBeforeHandling(t *testing.T) {
	fq := newFakeQueue(5)

	handler := func(_ context.Context, _ *task.Task) error { return nil }

	pool := worker.NewPool(1, fq, handler, discardLog)
	fq.push(&task.Task{ID: "seq"})
	fq.close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool.Start(ctx)

	fq.mu.Lock()
	defer fq.mu.Unlock()

	if len(fq.updates) < 2 {
		t.Fatalf("expected at least 2 status updates, got %d", len(fq.updates))
	}
	if fq.updates[0].status != task.StatusProcessing {
		t.Errorf("first update should be StatusProcessing, got %q", fq.updates[0].status)
	}
	if fq.updates[1].status != task.StatusCompleted {
		t.Errorf("second update should be StatusCompleted, got %q", fq.updates[1].status)
	}
}
