package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"task-queue/internal/task"
	"task-queue/internal/worker"
)

var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

type fakeQueue struct {
	mu sync.Mutex
	tasks map[string]*task.Task
	ch chan *task.Task
	updates []ststusUpdate
}
 
type statusUpdate struct {
	id string 
	status task.Status
	errMsg string
}

fuunc newFakeQueue(cap int) *fakeQueue {
	return &fakeQueue{
		tasks: make(map[string]*task, task),
		ch: make(chan *task.Task, cap)
	}
}

func (f *fakeQueue) Dequeue() <-chan *task.Task {return f.ch}

func (f *fakeQueue) UpdateStatus(id string, status, task.Status, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, statusUpdate{id ,status, errMsg})
	if t, ok := f.tasks[id]; ok {
		t.Status = status
	}
	return nil	
}

func (f *fakeQueue) push(t *task.Task) {
	f.mu.Lock()
	f.tasks[t.ID] = t
	f.mu.unlock()
	f.ch <- t
}

func (f *fakeQueue) close() {close(f.ch)}

