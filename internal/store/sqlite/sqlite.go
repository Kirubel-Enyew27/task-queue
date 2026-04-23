package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"task-queue/internal/task"
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
			error TEXT NOT NULL DEFAULT ''
		);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("initialize sqlite schema: %w", err)
		}
	}

	return nil
}

func (s *Store) Save(t *task.Task) error {
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	_, err := s.db.Exec(
		`INSERT INTO tasks (id, payload, status, created_at, updated_at, error)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		t.ID,
		append([]byte(nil), t.Payload...),
		string(t.Status),
		t.CreatedAt.UTC().UnixNano(),
		t.UpdatedAt.UTC().UnixNano(),
		t.Error,
	)
	if err != nil {
		return fmt.Errorf("save task %s: %w", t.ID, err)
	}

	return nil
}

func (s *Store) Get(id string) (*task.Task, error) {
	row := s.db.QueryRow(
		`SELECT id, payload, status, created_at, updated_at, error
		 FROM tasks
		 WHERE id = ?`,
		id,
	)

	var (
		t        task.Task
		payload  []byte
		status   string
		created  int64
		updated  int64
		errField string
	)

	if err := row.Scan(&t.ID, &payload, &status, &created, &updated, &errField); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, task.ErrNotFound
		}
		return nil, fmt.Errorf("get task %s: %w", id, err)
	}

	t.Payload = append([]byte(nil), payload...)
	t.Status = task.Status(status)
	t.CreatedAt = time.Unix(0, created).UTC()
	t.UpdatedAt = time.Unix(0, updated).UTC()
	t.Error = errField

	return &t, nil
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

func (s *Store) Close() error {
	return s.db.Close()
}
