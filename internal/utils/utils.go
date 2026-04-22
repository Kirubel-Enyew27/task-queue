package utils

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"task-queue/internal/task"
	"time"
)

type taskResponse struct {
	ID        string          `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	Status    task.Status     `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Error     string          `json:"error,omitempty"`
}

func DecodeJSON(body io.ReadCloser, dst any) error {
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

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func NewTaskID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.00000000"), ".", "")
	}
	return hex.EncodeToString(b[:])
}

func NewTaskResponse(t *task.Task) taskResponse {
	return taskResponse{
		ID:        t.ID,
		Payload:   json.RawMessage(append([]byte(nil), t.Payload...)),
		Status:    t.Status,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
		Error:     t.Error,
	}
}
