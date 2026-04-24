package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
	"task-queue/internal/task"
	"time"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func initSchema(db *sql.DB) error {
	stmts := []string{
		`PRAGMA foreign_keys = ON;`,
		`PRAGMA journal_mode = WAL;`,
		`CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			payload BLOB NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			next_run_at INTEGER NOT NULL DEFAULT 0,
			idempotency_key TEXT,
			dead_lettered_at INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency_key
			ON tasks(idempotency_key)
			WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at ON tasks(next_run_at);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("initialize sqlite schema: %w", err)
		}
	}

	return ensureTaskColumns(db)
}

func ensureTaskColumns(db *sql.DB) error {
	cols := map[string]bool{}
	rows, err := db.Query(`PRAGMA table_info(tasks);`)
	if err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    any
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("scan sqlite schema: %w", err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite schema: %w", err)
	}

	migrations := []struct {
		name string
		stmt string
	}{
		{"retry_count", `ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;`},
		{"max_retries", `ALTER TABLE tasks ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 3;`},
		{"next_run_at", `ALTER TABLE tasks ADD COLUMN next_run_at INTEGER NOT NULL DEFAULT 0;`},
		{"idempotency_key", `ALTER TABLE tasks ADD COLUMN idempotency_key TEXT;`},
		{"dead_lettered_at", `ALTER TABLE tasks ADD COLUMN dead_lettered_at INTEGER NOT NULL DEFAULT 0;`},
	}

	for _, mig := range migrations {
		if cols[mig.name] {
			continue
		}
		if _, err := db.Exec(mig.stmt); err != nil {
			return fmt.Errorf("migrate sqlite column %s: %w", mig.name, err)
		}
	}

	return nil
}

func (s *Store) Save(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}
	normalizeTask(t)

	_, err := s.db.Exec(
		`INSERT INTO tasks (id, payload, status, created_at, updated_at, error, retry_count, max_retries, next_run_at, idempotency_key, dead_lettered_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID,
		append([]byte(nil), t.Payload...),
		string(t.Status),
		t.CreatedAt.UTC().UnixNano(),
		t.UpdatedAt.UTC().UnixNano(),
		t.Error,
		t.RetryCount,
		t.MaxRetries,
		unixNanoOrZero(t.NextRunAt),
		nullString(t.IdempotencyKey),
		unixNanoOrZero(t.DeadLetteredAt),
	)
	if err != nil {
		return fmt.Errorf("save task %s: %w", t.ID, err)
	}
	return nil
}

func (s *Store) ListByStatus(status task.Status) ([]*task.Task, error) {
	rows, err := s.db.Query(
		`SELECT id, payload, status, created_at, updated_at, error, retry_count, max_retries, next_run_at, idempotency_key, dead_lettered_at
		FROM tasks
		WHERE status = ?
		ORDER BY created_at ASC`,
		string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("list tasks by status %q: %w", status, err)
	}
	defer rows.Close()

	tasks := make([]*task.Task, 0)
	for rows.Next() {
		t, err := scanTask(rows.Scan)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task rows: %w", err)
	}

	return tasks, nil
}

func (s *Store) ClaimAvailable(id string, staleAfter time.Duration) (bool, error) {
	now := time.Now().UTC()
	query := `UPDATE tasks
		SET status = ?, error = '', updated_at = ?
		WHERE id = ? AND (
			(status IN (?, ?) AND next_run_at <= ?)
			OR (status = ? AND updated_at <= ?)
		)`
	args := []any{
		string(task.StatusProcessing),
		now.UnixNano(),
		id,
		string(task.StatusPending),
		string(task.StatusRetrying),
		now.UnixNano(),
		string(task.StatusProcessing),
		now.Add(-staleAfter).UnixNano(),
	}
	if staleAfter <= 0 {
		query = `UPDATE tasks
			SET status = ?, error = '', updated_at = ?
			WHERE id = ? AND status IN (?, ?) AND next_run_at <= ?`
		args = []any{
			string(task.StatusProcessing),
			now.UnixNano(),
			id,
			string(task.StatusPending),
			string(task.StatusRetrying),
			now.UnixNano(),
		}
	}

	res, err := s.db.Exec(query, args...)
	if err != nil {
		return false, fmt.Errorf("claim task %s: %w", id, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim task %s rows affected: %w", id, err)
	}
	if affected == 0 {
		return false, nil
	}

	return true, nil
}

func (s *Store) Get(id string) (*task.Task, error) {
	row := s.db.QueryRow(
		`SELECT id, payload, status, created_at, updated_at, error, retry_count, max_retries, next_run_at, idempotency_key, dead_lettered_at
		FROM tasks
		WHERE id = ?`,
		id,
	)

	t, err := scanTask(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, task.ErrNotFound
		}
		return nil, fmt.Errorf("get task %s: %w", id, err)
	}

	return t, nil
}

func (s *Store) GetByIdempotencyKey(key string) (*task.Task, error) {
	if key == "" {
		return nil, task.ErrNotFound
	}

	row := s.db.QueryRow(
		`SELECT id, payload, status, created_at, updated_at, error, retry_count, max_retries, next_run_at, idempotency_key, dead_lettered_at
		FROM tasks
		WHERE idempotency_key = ?`,
		key,
	)

	t, err := scanTask(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, task.ErrNotFound
		}
		return nil, fmt.Errorf("get task by idempotency key %q: %w", key, err)
	}

	return t, nil
}

func (s *Store) UpdateStatus(id string, status task.Status, errMsg string) error {
	res, err := s.db.Exec(
		`UPDATE tasks
		SET status = ?, error = ?, updated_at = ?
		WHERE id = ?`,
		string(status),
		errMsg,
		time.Now().UTC().UnixNano(),
		id,
	)
	if err != nil {
		return fmt.Errorf("update task %s: %w", id, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update task %s rows affected: %w", id, err)
	}
	if affected == 0 {
		return task.ErrNotFound
	}
	return nil
}

func (s *Store) Update(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}
	normalizeTask(t)

	res, err := s.db.Exec(
		`UPDATE tasks
		SET payload = ?, status = ?, created_at = ?, updated_at = ?, error = ?, retry_count = ?, max_retries = ?, next_run_at = ?, idempotency_key = ?, dead_lettered_at = ?
		WHERE id = ?`,
		append([]byte(nil), t.Payload...),
		string(t.Status),
		t.CreatedAt.UTC().UnixNano(),
		t.UpdatedAt.UTC().UnixNano(),
		t.Error,
		t.RetryCount,
		t.MaxRetries,
		unixNanoOrZero(t.NextRunAt),
		nullString(t.IdempotencyKey),
		unixNanoOrZero(t.DeadLetteredAt),
		t.ID,
	)
	if err != nil {
		return fmt.Errorf("update task %s: %w", t.ID, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update task %s rows affected: %w", t.ID, err)
	}
	if affected == 0 {
		return task.ErrNotFound
	}
	return nil
}

func (s *Store) Delete(id string) error {
	res, err := s.db.Exec(`DELETE FROM tasks WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete task %s rows affected: %w", id, err)
	}
	if affected == 0 {
		return task.ErrNotFound
	}

	return nil
}

func normalizeTask(t *task.Task) {
	if t == nil {
		return
	}
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

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func unixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func scanTask(scan func(dest ...any) error) (*task.Task, error) {
	var (
		t              task.Task
		payload        []byte
		status         string
		created        int64
		updated        int64
		errField       string
		retryCount     int
		maxRetries     int
		nextRunAt      int64
		idempotencyKey sql.NullString
		deadLetteredAt int64
	)

	if err := scan(
		&t.ID,
		&payload,
		&status,
		&created,
		&updated,
		&errField,
		&retryCount,
		&maxRetries,
		&nextRunAt,
		&idempotencyKey,
		&deadLetteredAt,
	); err != nil {
		return nil, err
	}

	t.Payload = append([]byte(nil), payload...)
	t.Status = task.Status(status)
	t.CreatedAt = time.Unix(0, created).UTC()
	t.UpdatedAt = time.Unix(0, updated).UTC()
	t.Error = errField
	t.RetryCount = retryCount
	t.MaxRetries = maxRetries
	if nextRunAt > 0 {
		t.NextRunAt = time.Unix(0, nextRunAt).UTC()
	}
	if idempotencyKey.Valid {
		t.IdempotencyKey = idempotencyKey.String
	}
	if deadLetteredAt > 0 {
		t.DeadLetteredAt = time.Unix(0, deadLetteredAt).UTC()
	}

	return &t, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}
