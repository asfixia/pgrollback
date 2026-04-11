package tstproxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestPrepareFailedTableNotExistDoesNotAbortTransaction exercises the Prepare-guard fix:
// when the extended-query Parse step fails (e.g. the table does not exist), the proxy must
// roll back to its internal savepoint so the base transaction remains usable for subsequent
// queries — no SQLSTATE 25P02 "current transaction is aborted".
func TestPrepareFailedTableNotExistDoesNotAbortTransaction(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	testID := "test_prepare_guard_no_table"
	db, ctx, cleanup := connectToProxyForTest(t, testID)
	defer cleanup()
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(2 * time.Minute)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	nonExistentTable := fmt.Sprintf("pgrollback_no_such_table_%d", time.Now().UnixNano())

	// Attempt a parameterised SELECT on a non-existent table (triggers extended-query Prepare).
	// The driver uses the extended-query protocol for QueryContext with params.
	var dummy int
	err := db.QueryRowContext(qctx,
		fmt.Sprintf("SELECT 1 FROM %s WHERE id = $1 LIMIT 1", nonExistentTable), 0).Scan(&dummy)
	// We expect an error (42P01 table not found), but not a panic or connection loss.
	if err == nil {
		t.Fatalf("expected error for query on non-existent table, got nil")
	}
	t.Logf("Expected error from non-existent table (OK): %v", err)

	// The critical assertion: the session must still be usable — no "current transaction is aborted".
	var n int
	if scanErr := db.QueryRowContext(qctx, "SELECT 1").Scan(&n); scanErr != nil {
		t.Fatalf("SELECT 1 after failed Prepare should succeed, got: %v", scanErr)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}

	// Run a second bad query to confirm the guard fires consistently.
	anotherMissingTable := fmt.Sprintf("pgrollback_also_gone_%d", time.Now().UnixNano())
	var dummy2 int
	_ = db.QueryRowContext(qctx,
		fmt.Sprintf("SELECT id FROM %s WHERE id = $1", anotherMissingTable), 0).Scan(&dummy2)

	// Transaction must still be clean after the second failure.
	if scanErr := db.QueryRowContext(qctx, "SELECT 2").Scan(&n); scanErr != nil {
		t.Fatalf("SELECT 2 after second failed Prepare should succeed, got: %v", scanErr)
	}
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}

// TestPrepareGuardPreservesSuccessfulQueries verifies that queries on real tables continue
// to work normally after the guard is in place (no regression on the happy path).
func TestPrepareGuardPreservesSuccessfulQueries(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	testID := "test_prepare_guard_happy_path"
	db, ctx, cleanup := connectToProxyForTest(t, testID)
	defer cleanup()
	db.SetMaxOpenConns(1)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_guard_happy_%d", time.Now().UnixNano())

	if _, err := db.ExecContext(qctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (id INTEGER NOT NULL, val TEXT NOT NULL)`, tbl)); err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}
	if _, err := db.ExecContext(qctx,
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES ($1, $2), ($3, $4)`, tbl),
		1, "a", 2, "b"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Extended-query SELECT with parameter (uses Prepare under the hood).
	var count int
	if err := db.QueryRowContext(qctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id >= $1`, tbl), 1).Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}
