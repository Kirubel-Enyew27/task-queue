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
	ch     chan *task.Task
	store  map[string]*task.Task
	mu     sync.RWMutex
	once   sync.Once
	closed bool
	log    *slog.Logger
}

func New(capacity int, log *slog.Logger) *Queue {
	if log == nil {
		log = slog.Default()
	}

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
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	select {
	case q.ch <- t:
		q.store[t.ID] = t
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
	if !ok {
		return nil, false
	}
	return t.Clone(), true
}

func (q *Queue) Drain(ctx context.Context) {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.ch)
		q.mu.Unlock()
		q.log.Info("queue channel closed, draining...")
	})
	<-ctx.Done()
}
