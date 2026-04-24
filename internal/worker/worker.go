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
	concurrency    int
	pollInterval   time.Duration
	claimTimeout   time.Duration
	taskTimeout    time.Duration
	maxRetries     int
	retryBaseDalay time.Duration
	retryMaxDelay  time.Duration
	store          store.TaskStore
	handler        HandlerFunc
	log            *slog.Logger
	wg             sync.WaitGroup
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
		concurrency:    concurrency,
		pollInterval:   pollInterval,
		claimTimeout:   time.Minute,
		taskTimeout:    30 * time.Second,
		maxRetries:     3,
		retryBaseDalay: 250 * time.Millisecond,
		retryMaxDelay:  30 * time.Second,
		store:          store,
		handler:        handler,
		log:            log,
	}
}

func (p *Pool) SetTaskTimeout(timeout time.Duration) {
	if timeout > 0 {
		p.taskTimeout = timeout
	}
}

func (p *Pool) SetRetryPolicy(maxRetries int, baseDelay, maxDelay time.Duration) {
	if maxRetries >= 0 {
		p.maxRetries = maxRetries
	}
	if baseDelay > 0 {
		p.retryBaseDalay = baseDelay
	}
	if maxDelay > 0 {
		p.retryMaxDelay = maxDelay
	}
}

func (p *Pool) SetClaimTimeout(timeout time.Duration) {
	if timeout > 0 {
		p.claimTimeout = timeout
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
	pending, err := p.store.ListByStatus(task.StatusPending)
	if err != nil {
		return nil, err
	}
	retrying, err := p.store.ListByStatus(task.StatusRetrying)
	if err != nil {
		return nil, err
	}
	processing, err := p.store.ListByStatus(task.StatusProcessing)
	if err != nil {
		return nil, err
	}

	tasks := append(pending, append(retrying, processing...)...)
	for _, t := range tasks {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		claimed, err := p.store.ClaimAvailable(t.ID, p.claimTimeout)
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

	// err := p.handler(ctx, t)
	taskCtx := ctx
	cancel := func() {}
	if p.taskTimeout > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, p.taskTimeout)
	}
	defer cancel()

	err := p.handler(taskCtx, t)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			log.Info("task interrupted by shutdown", "id", t.ID)
			return
		}
		var errMsg string
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			errMsg = "task canceled: " + err.Error()
		} else {
			errMsg = err.Error()
		}
		p.handleFailure(log, t, errMsg)
		return
	}

	log.Info("task completed", "id", t.ID)
	t.Status = task.StatusCompleted
	t.Error = ""
	t.UpdatedAt = time.Now().UTC()
	t.NextRunAt = t.UpdatedAt
	_ = p.store.Update(t)
}

func (p *Pool) handleFailure(log *slog.Logger, t *task.Task, errMsg string) {
	now := time.Now().UTC()
	effectiveMax := p.effectiveMaxRetries(t)
	t.RetryCount++
	t.Error = errMsg
	t.UpdatedAt = now

	if t.RetryCount <= effectiveMax {
		t.Status = task.StatusRetrying
		t.NextRunAt = now.Add(p.retryDelay(t.RetryCount))
		if err := p.store.Update(t); err != nil {
			log.Error("failed to schedule retry", "id", t.ID, "err", err)
			return
		}
		log.Warn("task scheduled for retry", "id", t.ID, "attempt", t.RetryCount, "next_run_at", t.NextRunAt)
		return
	}

	t.Status = task.StatusDeadLettered
	t.DeadLetteredAt = now
	if err := p.store.Update(t); err != nil {
		log.Error("failed to dead-letter task", "id", t.ID, "err", err)
		return
	}
	log.Error("task dead-lettered", "id", t.ID, "attempts", t.RetryCount, "err", errMsg)
}

func (p *Pool) effectiveMaxRetries(t *task.Task) int {
	if t!= nil && t.MaxRetries > 0 {
		return t.MaxRetries
	}
	return p.maxRetries
}

func (p *Pool) retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := p.retryBaseDalay
	for i:= 1; i < attempt; i++ {
		delay *= 2
		if delay >= p.retryMaxDelay {
			return p.retryMaxDelay
		}
	}

	if delay > p.retryMaxDelay {
		return p.retryMaxDelay
	}
	return delay
}
