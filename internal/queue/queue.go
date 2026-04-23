package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"task-queue/internal/store"
	"task-queue/internal/store/memory"
	"task-queue/internal/task"
	"time"
)

type Queue struct {
	ch     chan *task.Task
	store  store.TaskStore
	mu     sync.RWMutex
	once   sync.Once
	closed bool
	log    *slog.Logger
}

func New(capacity int, log *slog.Logger) *Queue {
	return NewWithStore(capacity, memory.New(), log)
}

func NewWithStore(capacity int, taskStore store.TaskStore, log *slog.Logger) *Queue {
	if log == nil {
		log = slog.Default()
	}
	if taskStore == nil {
		taskStore = memory.New()
	}

	return &Queue{
		ch:    make(chan *task.Task, capacity),
		store: taskStore,
		log:   log,
	}
}

func (q *Queue) Enqueue(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	t.Status = task.StatusPending
	t.CreatedAt = time.Now().UTC()
	t.UpdatedAt = t.CreatedAt

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	if err := q.store.Save(t); err != nil {
		return err
	}

	select {
	case q.ch <- t:
		q.log.Info("task enqueued", "id", t.ID)
		return nil
	default:
		if err := q.store.Delete(t.ID); err != nil {
			q.log.Error("failed to roll back task after queue saturation", "id", t.ID, "err", err)
		}
		return fmt.Errorf("queue is full, capacity=%d", cap(q.ch))
	}
}

func (q *Queue) Restore(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	select {
	case q.ch <- t.Clone():
		q.log.Info("task restored into queue", "id", t.ID)
		return nil
	default:
		return fmt.Errorf("queue is full, capacity=%d", cap(q.ch))
	}
}

func (q *Queue) Dequeue() <-chan *task.Task {
	return q.ch
}
func (q *Queue) UpdateStatus(id string, status task.Status, errMsg string) error {
	if err := q.store.UpdateStatus(id, status, errMsg); err != nil {
		if errors.Is(err, task.ErrNotFound) {
			return fmt.Errorf("task not found: %s", id)
		}
		return err
	}
	return nil
}

func (q *Queue) Get(id string) (*task.Task, error) {
	t, err := q.store.Get(id)
	if err != nil {
		if errors.Is(err, task.ErrNotFound) {
			return nil, task.ErrNotFound
		}
		return nil, err
	}
	return t.Clone(), nil
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
