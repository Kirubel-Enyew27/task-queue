package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"task-queue/internal/queue"
	"task-queue/internal/store/memory"
	"task-queue/internal/task"
	"task-queue/internal/worker"
)

var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func waitForStatus(t *testing.T, q *queue.Queue, id string, want task.Status, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := q.Get(id)
		if err == nil && got.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := q.Get(id)
	if err != nil {
		t.Fatalf("get task %s: %v", id, err)
	}
	t.Fatalf("task %s status = %q, want %q", id, got.Status, want)
}

func TestPool_ProcessesTasksSuccessfully(t *testing.T) {

	store := memory.New()
	q := queue.NewWithStore(0, store, discardLog)
	for _, id := range []string{"w1", "w2"} {
		if err := q.Enqueue(&task.Task{ID: id, Payload: []byte(`{"job":"demo"}`)}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	var processed atomic.Int32
	handler := func(_ context.Context, tk *task.Task) error {
		processed.Add(1)
		return nil
	}

	pool := worker.NewPool(2, 10*time.Millisecond, store, handler, discardLog)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	waitForStatus(t, q, "w1", task.StatusCompleted, 2*time.Second)
	waitForStatus(t, q, "w2", task.StatusCompleted, 2*time.Second)

	cancel()
	<-done

	if got := processed.Load(); got != 2 {
		t.Fatalf("processed = %d, want 2", got)
	}
}

func TestPool_RetriesFailedTask(t *testing.T) {
	store := memory.New()
	q := queue.NewWithStore(0, store, discardLog)
	if err := q.Enqueue(&task.Task{ID: "bad-task", Payload: []byte(`{"job":"fail"}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var attempts atomic.Int32
	handler := func(_ context.Context, _ *task.Task) error {
		if attempts.Add(1) == 1 {
			return errors.New("simulated failure")
		}
		return nil
	}

	pool := worker.NewPool(1, 10*time.Millisecond, store, handler, discardLog)
	pool.SetRetryPolicy(1, 200*time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	waitForStatus(t, q, "bad-task", task.StatusRetrying, 2*time.Second)
	waitForStatus(t, q, "bad-task", task.StatusCompleted, 2*time.Second)

	cancel()
	<-done
}

func TestPool_DeadLettersAfterRetriesExhausted(t *testing.T) {
	store := memory.New()
	q := queue.NewWithStore(0, store, discardLog)
	if err := q.Enqueue(&task.Task{ID: "dead-task", Payload: []byte(`{"job":"fail"}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	handler := func(_ context.Context, _ *task.Task) error {
		return errors.New("simulated failure")
	}

	pool := worker.NewPool(1, 10*time.Millisecond, store, handler, discardLog)
	pool.SetRetryPolicy(0, 10*time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	waitForStatus(t, q, "dead-task", task.StatusDeadLettered, 2*time.Second)

	cancel()
	<-done
}

func TestPool_TaskTimeoutSchedulesRetry(t *testing.T) {
	store := memory.New()
	q := queue.NewWithStore(0, store, discardLog)
	if err := q.Enqueue(&task.Task{ID: "timeout-task", Payload: []byte(`{"job":"slow"}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handler := func(ctx context.Context, _ *task.Task) error {
		<-ctx.Done()
		return ctx.Err()
	}

	pool := worker.NewPool(1, 10*time.Millisecond, store, handler, discardLog)
	pool.SetTaskTimeout(20 * time.Millisecond)
	pool.SetRetryPolicy(0, 10*time.Millisecond, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	waitForStatus(t, q, "timeout-task", task.StatusDeadLettered, 2*time.Second)

	cancel()
	<-done
}

func TestPool_RespectsConcurrencyLimit(t *testing.T) {
	store := memory.New()
	q := queue.NewWithStore(0, store, discardLog)
	for i := 0; i < 4; i++ {
		id := taskID(i)
		if err := q.Enqueue(&task.Task{ID: id, Payload: []byte(`{"job":"slow"}`)}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	handler := func(_ context.Context, _ *task.Task) error {
		cur := inFlight.Add(1)
		for {
			max := maxInFlight.Load()
			if cur <= max || maxInFlight.CompareAndSwap(max, cur) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}

	pool := worker.NewPool(2, 10*time.Millisecond, store, handler, discardLog)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	for i := 0; i < 4; i++ {
		waitForStatus(t, q, taskID(i), task.StatusCompleted, 3*time.Second)
	}

	cancel()
	<-done

	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("max in-flight = %d, want <= 2", got)
	}
}

func taskID(i int) string {
	return string(rune('a' + i))
}
