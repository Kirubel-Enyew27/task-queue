package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"task-queue/internal/task"
)

type TaskQueue interface {
	Enqueue(t *task.Task) error
	Get(id string) (*task.Task, bool)
}

type Handler struct {
	queue TaskQueue
	log   *slog.Logger
}

type createTaskRequest struct {
	Payload json.RawMessage `json:"payload"`
}

type taskResponse struct {
	ID        string          `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	Status    task.Status     `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Error     string          `json:"error,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewHandler(queue TaskQueue, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}

	h := &Handler{
		queue: queue,
		log:   log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", h.createTask)
	mux.HandleFunc("GET /tasks/{id}", h.getTask)
	return mux
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	if len(req.Payload) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "payload is required"})
		return
	}

	t := &task.Task{
		ID:      newTaskID(),
		Payload: append([]byte(nil), req.Payload...),
	}

	if err := h.queue.Enqueue(t); err != nil {
		h.log.Error("failed to enqueue task", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}

	h.log.Info("task created via api", "id", t.ID)
	writeJSON(w, http.StatusAccepted, newTaskResponse(t))
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "task id is required"})
		return
	}

	t, ok := h.queue.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "task not found"})
		return
	}

	writeJSON(w, http.StatusOK, newTaskResponse(t))
}

func newTaskResponse(t *task.Task) taskResponse {
	return taskResponse{
		ID:        t.ID,
		Payload:   json.RawMessage(append([]byte(nil), t.Payload...)),
		Status:    t.Status,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
		Error:     t.Error,
	}
}

func decodeJSON(body io.ReadCloser, dst any) error {
	defer body.Close()

	dec := json.NewDecoder(io.LimitReader(body, 1<<20))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is required")
		}
		return errors.New("invalid json body")
	}

	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("request body must contain a single json object")
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newTaskID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	}

	return hex.EncodeToString(b[:])
}
