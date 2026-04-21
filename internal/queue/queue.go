// Package queue provides an in-memory, channel-based task queue.
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"task-queue/internal/task"
)

// Queue is a thread-safe in-memory task queue backed by a buffered channel.
type Queue struct {
	ch    chan *task.Task
	store map[string]*task.Task
	mu    sync.RWMutex
	log   *slog.Logger
}

// New creates a Queue with the given buffer capacity.
func New(capacity int, log *slog.Logger) *Queue {
	return &Queue{
		ch:    make(chan *task.Task, capacity),
		store: make(map[string]*task.Task),
		log:   log,
	}
}

// Enqueue adds a task to the queue. Returns an error if the queue is full.
func (q *Queue) Enqueue(t *task.Task) error {
	t.Status = task.StatusPending
	t.CreatedAt = time.Now()
	t.UpdatedAt = t.CreatedAt

	q.mu.Lock()
	q.store[t.ID] = t
	q.mu.Unlock()

	select {
	case q.ch <- t:
		q.log.Info("task enqueued", "id", t.ID)
		return nil
	default:
		return fmt.Errorf("queue is full, capacity=%d", cap(q.ch))
	}
}

// Dequeue returns a receive-only channel that workers consume from.
func (q *Queue) Dequeue() <-chan *task.Task {
	return q.ch
}

// UpdateStatus atomically updates a task's status in the store.
func (q *Queue) UpdateStatus(id string, status task.Status, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.store[id]
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}
	t.Status = status
	t.UpdatedAt = time.Now()
	t.Error = errMsg
	return nil
}

// Get retrieves a task by ID.
func (q *Queue) Get(id string) (*task.Task, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	t, ok := q.store[id]
	return t, ok
}

// Drain closes the underlying channel and blocks until all pending tasks are
// consumed or ctx is cancelled. Call this during shutdown.
func (q *Queue) Drain(ctx context.Context) {
	close(q.ch)
	q.log.Info("queue channel closed, draining...")
	<-ctx.Done()
}
