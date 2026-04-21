// Package worker provides a concurrent worker pool that processes tasks from a Queue.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"task-queue/internal/task"
)

// HandlerFunc is the function signature for executing a task's payload.
// Return a non-nil error to mark the task as failed.
type HandlerFunc func(ctx context.Context, t *task.Task) error

// StatusUpdater is satisfied by queue.Queue — keeps worker decoupled from queue.
type StatusUpdater interface {
	UpdateStatus(id string, status task.Status, errMsg string) error
	Dequeue() <-chan *task.Task
}

// Pool manages a fixed number of goroutines that pull tasks from the queue.
type Pool struct {
	concurrency int
	updater     StatusUpdater
	handler     HandlerFunc
	log         *slog.Logger
	wg          sync.WaitGroup
}

// NewPool creates a worker pool. concurrency sets how many workers run in parallel.
func NewPool(concurrency int, updater StatusUpdater, handler HandlerFunc, log *slog.Logger) *Pool {
	return &Pool{
		concurrency: concurrency,
		updater:     updater,
		handler:     handler,
		log:         log,
	}
}

// Start launches the worker goroutines. It returns when ctx is cancelled and
// all in-flight tasks have finished.
func (p *Pool) Start(ctx context.Context) {
	p.log.Info("worker pool starting", "concurrency", p.concurrency)
	ch := p.updater.Dequeue()

	for i := range p.concurrency {
		p.wg.Add(1)
		go p.run(ctx, i, ch)
	}

	p.wg.Wait()
	p.log.Info("worker pool stopped")
}

func (p *Pool) run(ctx context.Context, id int, ch <-chan *task.Task) {
	defer p.wg.Done()
	log := p.log.With("worker_id", id)
	log.Info("worker started")

	for {
		select {
		case t, ok := <-ch:
			if !ok {
				// Channel closed — queue is draining, shut down cleanly.
				log.Info("worker shutting down (channel closed)")
				return
			}
			p.process(ctx, log, t)

		case <-ctx.Done():
			log.Info("worker shutting down (context cancelled)")
			return
		}
	}
}

func (p *Pool) process(ctx context.Context, log *slog.Logger, t *task.Task) {
	log.Info("processing task", "id", t.ID)

	if err := p.updater.UpdateStatus(t.ID, task.StatusProcessing, ""); err != nil {
		log.Error("failed to update status to processing", "id", t.ID, "err", err)
	}

	err := p.handler(ctx, t)
	if err != nil {
		var errMsg string
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			errMsg = "task cancelled: " + err.Error()
		} else {
			errMsg = err.Error()
		}
		log.Error("task failed", "id", t.ID, "err", errMsg)
		_ = p.updater.UpdateStatus(t.ID, task.StatusFailed, errMsg)
		return
	}

	log.Info("task completed", "id", t.ID)
	_ = p.updater.UpdateStatus(t.ID, task.StatusCompleted, "")
}
