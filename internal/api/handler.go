package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"task-queue/internal/task"
	"task-queue/internal/utils"
)

type TaskQueue interface {
	Enqueue(t *task.Task) error
	Get(id string) (*task.Task, error)
	GetByIdempotencyKey(key string) (*task.Task, error)
}

type Handler struct {
	queue TaskQueue
	log   *slog.Logger
}

type createTaskRequest struct {
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
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
	if err := utils.DecodeJSON(r.Body, &req); err != nil {
		utils.WriteJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	if len(req.Payload) == 0 {
		utils.WriteJSON(w, http.StatusBadRequest, errorResponse{Error: "Payload is required"})
		return
	}

	if req.IdempotencyKey != "" {
		if existing, err := h.queue.GetByIdempotencyKey(req.IdempotencyKey); err == nil {
			h.log.Info("task replayed via idempotency key", "id", existing.ID, "key", req.IdempotencyKey)
			utils.WriteJSON(w, http.StatusOK, utils.NewTaskResponse(existing))
			return
		} else if !errors.Is(err, task.ErrNotFound) {
			h.log.Error("failed to resolve idempotency key", "key", req.IdempotencyKey, "err", err)
			utils.WriteJSON(w, http.StatusInternalServerError, errorResponse{
				Error: "failed to create task",
			})
			return
		}
	}

	t := &task.Task{
		ID:             utils.NewTaskID(),
		Payload:        append([]byte(nil), req.Payload...),
		IdempotencyKey: req.IdempotencyKey,
	}

	if err := h.queue.Enqueue(t); err != nil {
		if req.IdempotencyKey != "" {
			if existing, lookupErr := h.queue.GetByIdempotencyKey(req.IdempotencyKey); lookupErr == nil {
				h.log.Info("task replayed after enqueue race", "id", existing.ID, "key", req.IdempotencyKey)
				utils.WriteJSON(w, http.StatusOK, utils.NewTaskResponse(existing))
				return
			}
		}
		h.log.Error("failed to enqueue task", "err", err)
		utils.WriteJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}

	h.log.Info("task created via api", "id", t.ID)
	utils.WriteJSON(w, http.StatusAccepted, utils.NewTaskResponse(t))
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		utils.WriteJSON(w, http.StatusBadRequest, errorResponse{Error: "task id is required"})
		return
	}

	t, err := h.queue.Get(id)
	if err != nil {
		if errors.Is(err, task.ErrNotFound) {
			utils.WriteJSON(w, http.StatusNotFound, errorResponse{Error: "task not found"})
			return
		}
		h.log.Error("failed to get task", "id", id, "err", err)
		utils.WriteJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to read task"})
		return
	}

	utils.WriteJSON(w, http.StatusOK, utils.NewTaskResponse(t))
}
