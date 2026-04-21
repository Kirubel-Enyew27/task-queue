package task

import (
	"time"
)

// Status represents the lifecycle state of a task.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Task is the core unit of work in the queue.
type Task struct {
	ID        string
	Payload   []byte
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time
	Error     string // populated if Status == StatusFailed
}
