package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"task-queue/internal/task"
)

type HandlerFunc func(ctx context.Context, t *task.Task) error

type StatusUpdater interface {
	UpdateStatus(id string, status task.Status, errMsg string) error
	Dequeue() <-chan *task.Task
}

type Pool struct {
	concurrency int
	updater     StatusUpdater
	handler     HandlerFunc
	log         *slog.Logger
	wg          sync.WaitGroup
}

func NewPool(concurrency int, updater StatusUpdater, handler HandlerFunc, log *slog.Logger) *Pool {
	return &Pool{
		concurrency: concurrency,
		updater:     updater,
		handler:     handler,
		log:         log,
	}
}

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
				log.Info("worker shutting down (channel closed)")
				return
			}
			p.process(ctx, log, t)

		case <-ctx.Done():
			log.Info("worker shutting down (conetxt cancelled)")
			return
		}
	}
}

func (p *Pool) process(ctx context.Context, log *slog.Logger, t *task.Task) {
	log.Info("processing task", "id", t.ID)

	if err := p.updater.UpdateStatus(t.ID, task.StatusProcessing, ""); err != nil {
		log.Error("failed to update status or processing", "id", t.ID, "err", err)
	}

	err := p.handler(ctx, t)
	if err != nil {
		var errMsg string
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			errMsg = "task canceled: " + err.Error()
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
