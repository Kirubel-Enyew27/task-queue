package task

import (
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Task struct {
	ID        string
	Payload   []byte
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time
	Error     string
}
