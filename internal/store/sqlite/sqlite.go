// Package sqlite implements the store.Store interface on top of SQLite in WAL
// mode. It provides schema migration, transactions with conditional updates,
// unique constraints, operation-result persistence, recovery verification, and
// the named transaction checkpoints used by deterministic recovery tests.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)

	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// Store is the SQLite-backed store.Store implementation.
type Store struct {
	db    *sql.DB
	hooks *store.Hooks
}

// Open opens (and migrates) a SQLite database at path, or an in-memory database
// when path is empty or ":memory:". A single open connection serializes writers
// while WAL mode enables snapshot readers and safe concurrent recovery.
func Open(path string, hooks *store.Hooks) (*Store, error) {
	dsn := dsnFor(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, hooks: hooks}, nil
}

// dsnFor builds a driver DSN for a file or in-memory database. WAL journal mode
// and foreign-key enforcement are enabled for file databases; busy timeout keeps
// concurrent writers from failing immediately on SQLite's single-writer lock.
func dsnFor(path string) string {
	if path == "" || path == ":memory:" {
		return "file:aliquotseal-memory?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	}
	q := url.Values{}
	q.Set("_pragma", "journal_mode(WAL)")
	q.Set("_pragma", "busy_timeout(5000)")
	q.Set("_pragma", "foreign_keys(1)")
	return "file:" + url.PathEscape(path) + "?" + q.Encode()
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// check consults the interrupt hook at a named checkpoint.
func (s *Store) check(c store.Checkpoint) error {
	if s.hooks != nil && s.hooks.Interrupt != nil {
		return s.hooks.Interrupt(c)
	}
	return nil
}

// barrier blocks at a named synchronization point when a hook is configured.
func (s *Store) barrier(name string) {
	if s.hooks != nil && s.hooks.Barrier != nil {
		s.hooks.Barrier(name)
	}
}
