package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"task-queue/internal/store"
	"task-queue/internal/task"
)

type HandlerFunc func(ctx context.Context, t *task.Task) error

type Pool struct {
	concurrency  int
	pollInterval time.Duration
	store        store.TaskStore
	handler      HandlerFunc
	log          *slog.Logger
	wg           sync.WaitGroup
}

func NewPool(concurrency int, pollInterval time.Duration, store store.TaskStore, handler HandlerFunc, log *slog.Logger) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	if log == nil {
		log = slog.Default()
	}

	return &Pool{
		concurrency:  concurrency,
		pollInterval: pollInterval,
		store:        store,
		handler:      handler,
		log:          log,
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.log.Info("worker pool starting", "concurrency", p.concurrency, "poll_interval", p.pollInterval.String())

	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.run(ctx, i)
	}

	p.wg.Wait()
	p.log.Info("worker pool stopped")
}

func (p *Pool) run(ctx context.Context, id int) {
	defer p.wg.Done()

	log := p.log.With("worker_id", id)
	log.Info("worker started")

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		t, err := p.nextTask(ctx)
		if err != nil {
			log.Error("poll failed", "err", err)
		}
		if t != nil {
			p.process(ctx, log, t)
			continue
		}

		select {
		case <-ctx.Done():
			log.Info("worker shutting down", "err", ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (p *Pool) nextTask(ctx context.Context) (*task.Task, error) {
	tasks, err := p.store.ListByStatus(task.StatusPending)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		claimed, err := p.store.ClaimPending(t.ID)
		if err != nil {
			return nil, err
		}
		if !claimed {
			continue
		}

		claimedTask := t.Clone()
		claimedTask.Status = task.StatusProcessing
		return claimedTask, nil
	}

	return nil, nil
}

func (p *Pool) process(ctx context.Context, log *slog.Logger, t *task.Task) {
	log.Info("processing task", "id", t.ID)

	err := p.handler(ctx, t)
	if err != nil {
		var errMsg string
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			errMsg = "task canceled: " + err.Error()
		} else {
			errMsg = err.Error()
		}
		log.Error("task failed", "id", t.ID, "err", errMsg)
		_ = p.store.UpdateStatus(t.ID, task.StatusFailed, errMsg)
		return
	}

	log.Info("task completed", "id", t.ID)
	_ = p.store.UpdateStatus(t.ID, task.StatusCompleted, "")
}
