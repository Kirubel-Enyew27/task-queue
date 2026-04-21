package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"task-queue/internal/task"
	"time"
)

type Queue struct {
	ch    chan *task.Task
	store map[string]*task.Task
	mu    sync.RWMutex
	log   *slog.Logger
}

func New(capacity int, log *slog.Logger) *Queue {
	return &Queue{
		ch:    make(chan *task.Task, capacity),
		store: make(map[string]*task.Task, 0),
		log:   log,
	}
}

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

func (q *Queue) Dequeue() <-chan *task.Task {
	return q.ch
}
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

func (q *Queue) Get(id string) (*task.Task, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	t, ok := q.store[id]
	return t, ok
}

func (q *Queue) Drain(ctx context.Context) {
	close(q.ch)
	q.log.Info("queue channel closed, draining...")
	<-ctx.Done()
}
