package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestEnvDurationUsesConfiguredWorkerPollInterval(t *testing.T) {
	t.Setenv("WORKER_POLL_INTERVAL", "2s")

	got := envDuration("WORKER_POLL_INTERVAL", 250*time.Millisecond)
	if got != 2*time.Second {
		t.Fatalf("envDuration = %v, want %v", got, 2*time.Second)
	}
}

func TestEnvDurationFallsBackOnInvalidValue(t *testing.T) {
	t.Setenv("WORKER_POLL_INTERVAL", "not-a-duration")

	got := envDuration("WORKER_POLL_INTERVAL", 250*time.Millisecond)
	if got != 250*time.Millisecond {
		t.Fatalf("envDuration = %v, want %v", got, 250*time.Millisecond)
	}
}

func TestTaskDBPathUsesEnvOverride(t *testing.T) {
	t.Setenv("TASK_QUEUE_DB_PATH", "/tmp/custom.db")

	if got := taskDBPath(); got != "/tmp/custom.db" {
		t.Fatalf("taskDBPath = %q, want %q", got, "/tmp/custom.db")
	}
}

func TestTaskDBPathDefaults(t *testing.T) {
	t.Setenv("TASK_QUEUE_DB_PATH", "")

	want := filepath.Join(".", "task_queue.db")
	if got := taskDBPath(); got != want {
		t.Fatalf("taskDBPath = %q, want %q", got, want)
	}
}
