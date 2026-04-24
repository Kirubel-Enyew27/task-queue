package memory

import (
	"fmt"
	"sync"
	"task-queue/internal/task"
	"time"
)

type Store struct {
	mu             sync.RWMutex
	tasks          map[string]*task.Task
	idempotencyIdx map[string]string
}

func New() *Store {
	return &Store{
		tasks:          make(map[string]*task.Task),
		idempotencyIdx: make(map[string]string),
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
	if t.IdempotencyKey != "" {
		if _, exists := s.idempotencyIdx[t.IdempotencyKey]; exists {
			return fmt.Errorf("task already exists for idempotency key: %s", t.IdempotencyKey)
		}
	}

	normalizeTask(t)
	s.tasks[t.ID] = cloneTask(t)
	if t.IdempotencyKey != "" {
		s.idempotencyIdx[t.IdempotencyKey] = t.ID
	}
	return nil
}

func (s *Store) Get(id string) (*task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, task.ErrNotFound
	}

	return cloneTask(t), nil
}

func (s *Store) GetByIdempotencyKey(key string) (*task.Task, error) {
	if key == "" {
		return nil, task.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.idempotencyIdx[key]
	if !ok {
		return nil, task.ErrNotFound
	}

	t, ok := s.tasks[id]
	if !ok {
		return nil, task.ErrNotFound
	}

	return cloneTask(t), nil
}

func (s *Store) ClaimAvailable(id string, staleAfter time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return false, task.ErrNotFound
	}
	if t.Status == task.StatusPending || t.Status == task.StatusRetrying {
		if !t.NextRunAt.IsZero() && time.Now().UTC().Before(t.NextRunAt) {
			return false, nil
		}
		t.Status = task.StatusProcessing
		t.UpdatedAt = time.Now().UTC()
		t.Error = ""
		return true, nil
	}

	if t.Status != task.StatusProcessing {
		return false, nil
	}

	if staleAfter <= 0 || time.Since(t.UpdatedAt) <= staleAfter {
		return false, nil
	}

	t.Status = task.StatusProcessing
	t.UpdatedAt = time.Now().UTC()
	t.Error = ""
	return true, nil
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

func (s *Store) Update(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.tasks[t.ID]
	if !ok {
		return task.ErrNotFound
	}

	if current.IdempotencyKey != "" && current.IdempotencyKey != t.IdempotencyKey {
		delete(s.idempotencyIdx, current.IdempotencyKey)
	}
	if t.IdempotencyKey != "" {
		s.idempotencyIdx[t.IdempotencyKey] = t.ID
	}

	normalizeTask(t)
	s.tasks[t.ID] = cloneTask(t)
	return nil
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return task.ErrNotFound
	}

	if t.IdempotencyKey != "" {
		delete(s.idempotencyIdx, t.IdempotencyKey)
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

func normalizeTask(t *task.Task) {
	if t.NextRunAt.IsZero() {
		t.NextRunAt = t.CreatedAt
	}
	if t.MaxRetries < 0 {
		t.MaxRetries = 0
	}
	if t.RetryCount < 0 {
		t.RetryCount = 0
	}
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
