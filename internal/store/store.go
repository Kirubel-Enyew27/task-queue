package store

import (
	"time"

	"task-queue/internal/task"
)

type TaskStore interface {
	Save(t *task.Task) error
	Get(id string) (*task.Task, error)
	GetByIdempotencyKey(key string) (*task.Task, error)
	ClaimAvailable(id string, staleAfter time.Duration) (bool, error)
	UpdateStatus(id string, status task.Status, errMsg string) error
	Update(t *task.Task) error
	Delete(id string) error
	ListByStatus(status task.Status) ([]*task.Task, error)
}
