package task

import (
	"errors"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

var ErrNotFound = errors.New("task not found")

type Task struct {
	ID        string
	Payload   []byte
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time
	Error     string
}

func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}

	cp := *t
	if t.Payload != nil {
		cp.Payload = append([]byte(nil), t.Payload...)
	}

	return &cp
}
