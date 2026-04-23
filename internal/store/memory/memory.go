package memory

import (
	"fmt"
	"sync"
	"task-queue/internal/task"
	"time"
)

type Store struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

func New() *Store {
	return &Store{
		tasks: make(map[string]*task.Task),
	}
}

func (s *Store) Save(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[t.ID]; exists {
		return fmt.Errorf("task already exists: %s", t.ID)
	}

	s.tasks[t.ID] = cloneTask(t)
	return nil
}

func (s *Store) Get(id string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, task.ErrNotFound
	}

	return cloneTask(t), nil
}

func (s *Store) UpdateStatus(id string, status task.Status, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return task.ErrNotFound
	}

	t.Status = status
	t.UpdatedAt = time.Now().UTC()
	t.Error = errMsg
	return nil
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tasks[id]; !ok {
		return task.ErrNotFound
	}

	delete(s.tasks, id)
	return nil
}

func (s *Store) ListByStatus(status task.Status) ([]*task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*task.Task, 0)
	for _, t := range s.tasks {
		if t.Status == status {
			tasks = append(tasks, cloneTask(t))
		}
	}

	return tasks, nil
}

func cloneTask(t *task.Task) *task.Task {
	if t == nil {
		return nil
	}

	cp := *t
	if t.Payload != nil {
		cp.Payload = append([]byte(nil), t.Payload...)
	}
	return &cp
}
