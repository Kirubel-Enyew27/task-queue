package store

import "task-queue/internal/task"

type TaskStore interface {
	Save(t *task.Task) error
	Get(id string) (*task.Task, error)
	UpdateStatus(id string, status task.Status, errMsg string) error
	Delete(id string) error
}
