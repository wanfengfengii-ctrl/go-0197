package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// schema is the full data model described in PROJECT_SPEC data model section.
// It is applied idempotently inside a single migration transaction so a fresh
// database and a restarted database share one code path.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS specimens (
		sample_id   TEXT PRIMARY KEY,
		description TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS batch_revisions (
		sample_id TEXT NOT NULL,
		batch_id  TEXT NOT NULL,
		revision  TEXT NOT NULL,
		PRIMARY KEY (sample_id, batch_id, revision)
	)`,
	`CREATE TABLE IF NOT EXISTS containers (
		tube_id      TEXT PRIMARY KEY,
		type         TEXT NOT NULL,
		sample_id    TEXT NOT NULL,
		batch_id     TEXT NOT NULL,
		revision     TEXT NOT NULL,
		remaining_uL INTEGER NOT NULL,
		status       TEXT NOT NULL,
		session_id   TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS aliquot_sessions (
		session_id       TEXT PRIMARY KEY,
		mother_tube_id   TEXT NOT NULL,
		sample_id        TEXT NOT NULL,
		batch_id         TEXT NOT NULL,
		revision         TEXT NOT NULL,
		locked_volume_uL INTEGER NOT NULL,
		status           TEXT NOT NULL,
		revision_num     INTEGER NOT NULL,
		terminal_kind    TEXT NOT NULL DEFAULT '',
		terminal_reason  TEXT NOT NULL DEFAULT '',
		created_at       TEXT NOT NULL
	)`,
	// At most one non-terminal (open) session per mother tube (domain rule 3).
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_open_session_mother
		ON aliquot_sessions (mother_tube_id) WHERE terminal_kind = ''`,
	`CREATE TABLE IF NOT EXISTS planned_children (
		session_id        TEXT NOT NULL,
		ordinal           INTEGER NOT NULL,
		child_tube_id     TEXT NOT NULL UNIQUE,
		planned_volume_uL INTEGER NOT NULL,
		created           INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (session_id, ordinal)
	)`,
	`CREATE TABLE IF NOT EXISTS volume_entries (
		session_id  TEXT NOT NULL,
		seq         INTEGER NOT NULL,
		entry_type  TEXT NOT NULL,
		quantity_uL INTEGER NOT NULL,
		child_tube_id TEXT NOT NULL DEFAULT '',
		operation_id  TEXT NOT NULL,
		PRIMARY KEY (session_id, seq)
	)`,
	`CREATE TABLE IF NOT EXISTS lineage_edges (
		session_id    TEXT NOT NULL,
		parent_tube_id TEXT NOT NULL,
		child_tube_id  TEXT NOT NULL UNIQUE,
		allocation_uL  INTEGER NOT NULL,
		state          TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS refreeze_verifications (
		child_tube_id TEXT PRIMARY KEY,
		session_id    TEXT NOT NULL,
		sample_id     TEXT NOT NULL,
		batch_id      TEXT NOT NULL,
		revision      TEXT NOT NULL,
		allocation_uL INTEGER NOT NULL,
		freeze_run_id TEXT NOT NULL,
		operation_id  TEXT NOT NULL,
		verified_at   TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS session_outcomes (
		session_id    TEXT PRIMARY KEY,
		terminal_kind TEXT NOT NULL,
		revision      INTEGER NOT NULL,
		reason        TEXT NOT NULL,
		summary       TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS lineage_manifests (
		session_id        TEXT PRIMARY KEY,
		mother_tube_id    TEXT NOT NULL,
		sample_id         TEXT NOT NULL,
		batch_id          TEXT NOT NULL,
		revision          TEXT NOT NULL,
		frozen_volume_uL  INTEGER NOT NULL,
		total_loss_uL     INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS manifest_items (
		session_id    TEXT NOT NULL,
		ordinal       INTEGER NOT NULL,
		child_tube_id TEXT NOT NULL,
		allocation_uL INTEGER NOT NULL,
		freeze_run_id TEXT NOT NULL,
		PRIMARY KEY (session_id, ordinal)
	)`,
	`CREATE TABLE IF NOT EXISTS operation_results (
		operation_id TEXT PRIMARY KEY,
		fingerprint  TEXT NOT NULL,
		status_code  TEXT NOT NULL,
		message      TEXT NOT NULL DEFAULT '',
		revision     INTEGER NOT NULL,
		terminal     TEXT NOT NULL DEFAULT '',
		session_id   TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS session_events (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		session_id   TEXT NOT NULL,
		revision     INTEGER NOT NULL,
		command_type TEXT NOT NULL,
		result       TEXT NOT NULL,
		operation_id TEXT NOT NULL
	)`,
}

// migrate applies the schema within a single transaction.
func migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback()
	for _, stmt := range schema {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
