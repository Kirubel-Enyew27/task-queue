package queue

import (
	"errors"
	"fmt"
	"log/slog"
	"task-queue/internal/store"
	"task-queue/internal/store/memory"
	"task-queue/internal/task"
	"time"
)

type Queue struct {
	store store.TaskStore
	log   *slog.Logger
}

func New(capacity int, log *slog.Logger) *Queue {
	return NewWithStore(capacity, memory.New(), log)
}

func NewWithStore(_ int, taskStore store.TaskStore, log *slog.Logger) *Queue {
	if log == nil {
		log = slog.Default()
	}
	if taskStore == nil {
		taskStore = memory.New()
	}

	return &Queue{
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
	if t.MaxRetries <= 0 {
		t.MaxRetries = 3
	}
	t.RetryCount = 0
	if t.NextRunAt.IsZero() {
		t.NextRunAt = t.CreatedAt
	}

	if err := q.store.Save(t); err != nil {
		return err
	}
	q.log.Info("task enqueued", "id", t.ID)
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

func (q *Queue) GetByIdempotencyKey(key string) (*task.Task, error) {
	t, err := q.store.GetByIdempotencyKey(key)
	if err != nil {
		if errors.Is(err, task.ErrNotFound) {
			return nil, task.ErrNotFound
		}
		return nil, err
	}
	return t.Clone(), nil
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

func (q *Queue) Update(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}
	if err := q.store.Update(t); err != nil {
		if errors.Is(err, task.ErrNotFound) {
			return fmt.Errorf("task not found: %s", t.ID)
		}
		return err
	}
	return nil
}

func (q *Queue) Dequeue() <-chan *task.Task {
	return nil
}

func (q *Queue) Drain(_ any) {}
