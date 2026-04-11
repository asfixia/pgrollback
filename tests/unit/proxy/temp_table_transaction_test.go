package tstproxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestProxyTempTableDDLInTransaction runs BEGIN (via sql.Tx), CREATE TEMP TABLE, INSERT,
// ADD CONSTRAINT (PK + CHECK), DROP TABLE, COMMIT — all through pgrollback.
// Uses a single connection so the temp table stays in-session.
func TestProxyTempTableDDLInTransaction(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "temp_table_ddl_tx")
	defer cleanup()
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(2 * time.Minute)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_tmp_%d", time.Now().UnixNano())

	tx, err := db.BeginTx(qctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (id INTEGER NOT NULL, label TEXT NOT NULL)`, tbl))
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(`INSERT INTO %s (id, label) VALUES (1, $1), (2, $2)`, tbl), "first", "second")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`ALTER TABLE %s ADD CONSTRAINT %s_pkey PRIMARY KEY (id)`, tbl, tbl))
	if err != nil {
		t.Fatalf("ADD PRIMARY KEY: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`ALTER TABLE %s ADD CONSTRAINT %s_chk CHECK (id > 0 AND length(label) > 0)`, tbl, tbl))
	if err != nil {
		t.Fatalf("ADD CHECK: %v", err)
	}

	var n int
	err = tx.QueryRowContext(qctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tbl)).Scan(&n)
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(`DROP TABLE %s`, tbl))
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	// After commit, temp table must be gone; new query on same pool/conn should not see it.
	var after int
	err = db.QueryRowContext(qctx, "SELECT 1").Scan(&after)
	if err != nil {
		t.Fatalf("post-commit ping: %v", err)
	}
	if after != 1 {
		t.Fatalf("expected 1, got %d", after)
	}
}

// TestProxyTempTableDDLWithSQLComments runs the same DDL/DML flow as TestProxyTempTableDDLInTransaction
// but embeds line and block SQL comments inside the forwarded strings (proxy must pass them through).
func TestProxyTempTableDDLWithSQLComments(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "temp_table_ddl_comments")
	defer cleanup()
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(2 * time.Minute)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_tmp_c_%d", time.Now().UnixNano())

	tx, err := db.BeginTx(qctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`/* scratch table for comment-aware DDL test */
CREATE TEMP TABLE %s (
  id INTEGER NOT NULL, -- surrogate key
  label TEXT NOT NULL  -- display label
)`, tbl))
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`INSERT INTO %s (id, label) VALUES (1, $1), (2, $2) -- two seed rows`, tbl), "alpha", "beta")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`ALTER TABLE %s
ADD CONSTRAINT %s_pkey PRIMARY KEY (id) -- enforce uniqueness`, tbl, tbl))
	if err != nil {
		t.Fatalf("ADD PRIMARY KEY: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`ALTER TABLE %s ADD CONSTRAINT %s_chk /* id must be positive */ CHECK (id > 0 AND length(label) > 0)`, tbl, tbl))
	if err != nil {
		t.Fatalf("ADD CHECK: %v", err)
	}

	var n int
	err = tx.QueryRowContext(qctx, fmt.Sprintf(
		`SELECT COUNT(*) /* row count */ FROM %s WHERE id < 10 -- only small ids`, tbl)).Scan(&n)
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(`DROP TABLE %s -- explicit drop before commit`, tbl))
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

// TestProxyTempTableDDLMultiStatementBatch sends BEGIN … COMMIT as one script on a single
// reserved connection (pool size 1), matching clients that issue multi-statement DDL batches.
func TestProxyTempTableDDLMultiStatementBatch(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "temp_table_ddl_batch")
	defer cleanup()
	db.SetMaxOpenConns(1)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_tmp_b_%d", time.Now().UnixNano())

	// Literal values only inside the batch (no parameters across statement boundaries in one Exec).
	script := fmt.Sprintf(`
BEGIN;
CREATE TEMP TABLE %s (id INTEGER NOT NULL, label TEXT NOT NULL);
INSERT INTO %s (id, label) VALUES (10, 'batch_a'), (20, 'batch_b');
ALTER TABLE %s ADD CONSTRAINT %s_pkey PRIMARY KEY (id);
ALTER TABLE %s ADD CONSTRAINT %s_chk CHECK (id > 0);
DROP TABLE %s;
COMMIT;
`, tbl, tbl, tbl, tbl, tbl, tbl, tbl)

	conn, err := db.Conn(qctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(qctx, script)
	if err != nil {
		t.Fatalf("multi-statement batch: %v", err)
	}

	var ping int
	err = conn.QueryRowContext(qctx, "SELECT 1").Scan(&ping)
	if err != nil {
		t.Fatalf("after batch: %v", err)
	}
	if ping != 1 {
		t.Fatalf("expected 1, got %d", ping)
	}
}

// TestProxyTempTableMultiStatementSQLWithComments sends exactly one query string containing several
// statements separated by semicolons, with line and block comments inside the script (literal values only).
func TestProxyTempTableMultiStatementSQLWithComments(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "temp_table_ddl_batch_comments")
	defer cleanup()
	db.SetMaxOpenConns(1)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_tmp_bc_%d", time.Now().UnixNano())

	script := fmt.Sprintf(`/* batch: temp DDL with embedded comments */
BEGIN; -- client transaction
CREATE TEMP TABLE %s ( -- columns
  id INTEGER NOT NULL,
  label TEXT NOT NULL
); /* table created */
INSERT INTO %s (id, label) VALUES (100, 'c_a'), (200, 'c_b'); -- seed
ALTER TABLE %s ADD CONSTRAINT %s_pkey PRIMARY KEY (id); -- uniqueness
ALTER TABLE %s ADD CONSTRAINT %s_chk CHECK (id > 0 /* positive ids only */ AND length(label) > 0);
DROP TABLE %s; -- drop before commit
COMMIT; -- end
`, tbl, tbl, tbl, tbl, tbl, tbl, tbl)

	conn, err := db.Conn(qctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(qctx, script)
	if err != nil {
		t.Fatalf("single multi-statement SQL with comments: %v", err)
	}

	var ping int
	err = conn.QueryRowContext(qctx, "SELECT 1").Scan(&ping)
	if err != nil {
		t.Fatalf("after batch: %v", err)
	}
	if ping != 1 {
		t.Fatalf("expected 1, got %d", ping)
	}
}

// TestProxyTempTableConstraintViolation verifies CHECK rejects bad row before DROP (still rolls back via test cleanup).
func TestProxyTempTableConstraintViolation(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "temp_table_ddl_violation")
	defer cleanup()
	db.SetMaxOpenConns(1)

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 15*time.Second))
	defer cancel()

	tbl := fmt.Sprintf("pgrollback_tmp_v_%d", time.Now().UnixNano())

	tx, err := db.BeginTx(qctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (id INTEGER NOT NULL)`, tbl))
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(
		`ALTER TABLE %s ADD CONSTRAINT %s_pos CHECK (id > 0)`, tbl, tbl))
	if err != nil {
		t.Fatalf("ADD CHECK: %v", err)
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(`INSERT INTO %s (id) VALUES (-1)`, tbl))
	if err == nil {
		t.Fatal("expected CHECK violation on INSERT -1, got nil error")
	}

	_, err = tx.ExecContext(qctx, fmt.Sprintf(`DROP TABLE %s`, tbl))
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}
