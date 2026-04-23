package api_test

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"task-queue/internal/api"
	"task-queue/internal/queue"
	"task-queue/internal/task"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCreateTask(t *testing.T) {
	q := queue.New(10, testLogger())
	handler := api.NewHandler(q, testLogger())

	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":{"job":"demo","count":1}}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}

	var got struct {
		ID      string          `json:"id"`
		Payload json.RawMessage `json:"payload"`
		Status  task.Status     `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.ID == "" {
		t.Fatal("expected generated task id")
	}
	if got.Status != task.StatusPending {
		t.Fatalf("status = %q, want %q", got.Status, task.StatusPending)
	}
	if string(got.Payload) != `{"job":"demo","count":1}` {
		t.Fatalf("payload = %s", got.Payload)
	}

	stored, err := q.Get(got.ID)
	if err != nil {
		t.Fatalf("get stored task: %v", err)
	}
	if stored == nil {
		t.Fatal("task not stored in queue")
	}
	if string(stored.Payload) != `{"job":"demo","count":1}` {
		t.Fatalf("stored payload = %s", stored.Payload)
	}
}

func TestCreateTask_BadRequest(t *testing.T) {
	q := queue.New(10, testLogger())
	handler := api.NewHandler(q, testLogger())

	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetTask(t *testing.T) {
	q := queue.New(10, testLogger())
	tk := &task.Task{ID: "task-123", Payload: []byte(`{"job":"fetch"}`)}
	if err := q.Enqueue(tk); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}

	handler := api.NewHandler(q, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/tasks/task-123", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var got struct {
		ID      string          `json:"id"`
		Payload json.RawMessage `json:"payload"`
		Status  task.Status     `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.ID != "task-123" {
		t.Fatalf("id = %q, want %q", got.ID, "task-123")
	}
	if got.Status != task.StatusPending {
		t.Fatalf("status = %q, want %q", got.Status, task.StatusPending)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	q := queue.New(10, testLogger())
	handler := api.NewHandler(q, testLogger())

	req := httptest.NewRequest(http.MethodGet, "/tasks/missing", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetTask_StoreError(t *testing.T) {
	handler := api.NewHandler(failingQueue{}, testLogger())

	req := httptest.NewRequest(http.MethodGet, "/tasks/task-123", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

type failingQueue struct{}

func (failingQueue) Enqueue(*task.Task) error { return nil }

func (failingQueue) Get(string) (*task.Task, error) {
	return nil, errors.New("boom")
}
