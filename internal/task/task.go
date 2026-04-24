package task

import (
	"errors"
	"time"
)

type Status string

const (
	StatusPending      Status = "pending"
	StatusProcessing   Status = "processing"
	StatusRetrying     Status = "retrying"
	StatusCompleted    Status = "completed"
	StatusFailed       Status = "failed"
	StatusDeadLettered Status = "dead_lettered"
)

var ErrNotFound = errors.New("task not found")

type Task struct {
	ID             string
	Payload        []byte
	Status         Status
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Error          string
	RetryCount     int
	MaxRetries     int
	NextRunAt      time.Time
	IdempotencyKey string
	DeadLetteredAt time.Time
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
