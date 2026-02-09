// describe_wire_test.go: tests that INSERT ... RETURNING "id" through the proxy returns
// a result with one column "id" and one row (the inserted id). This is the client-visible
// behaviour required by Laravel/PHP PDO; if the proxy regresses (e.g. empty RowDescription),
// clients get no row and Laravel hits "Undefined array key 0". This test uses the normal
// client API (database/sql) instead of wire protocol internals.

package tstproxy

import (
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const describeReturningTestID = "describe_returning_wire"

// TestDescribeStatement_InsertReturning_ReturnsRowDescriptionWithId asserts that when a client
// runs INSERT INTO t (a) VALUES ($1) RETURNING "id" through the proxy, the result has exactly
// one column named "id" and one row with the inserted id. This guarantees the same behaviour
// that Laravel/PHP PDO rely on (processInsertGetId), without testing wire protocol internals.
func TestDescribeStatement_InsertReturning_ReturnsRowDescriptionWithId(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, describeReturningTestID)
	defer cleanup()

	// Create table (same as Laravel migration style)
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS t (id SERIAL PRIMARY KEY, a TEXT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Run INSERT ... RETURNING "id" as a client would (e.g. Laravel / PDO prepared statement)
	query := `INSERT INTO t (a) VALUES ($1) RETURNING "id"`
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("Prepare INSERT RETURNING: %v", err)
	}
	defer stmt.Close()

	var id int64
	err = stmt.QueryRowContext(ctx, "hello").Scan(&id)
	if err != nil {
		t.Fatalf("QueryRow INSERT RETURNING: %v (Laravel would get no row and 'Undefined array key 0')", err)
	}

	if id < 1 {
		t.Errorf("expected inserted id >= 1, got %d", id)
	}

	// Optionally verify result set shape via Query (one column "id", one row)
	rows, err := db.QueryContext(ctx, query, "second")
	if err != nil {
		t.Fatalf("Query INSERT RETURNING: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns(): %v", err)
	}
	if len(cols) != 1 {
		t.Errorf("result columns: want 1 (id), got %d: %v", len(cols), cols)
	}
	if len(cols) > 0 && cols[0] != "id" {
		t.Errorf("first column name: want \"id\", got %q", cols[0])
	}

	rowCount := 0
	for rows.Next() {
		var id2 int64
		if err := rows.Scan(&id2); err != nil {
			t.Fatalf("Scan row: %v", err)
		}
		rowCount++
		if id2 < 1 {
			t.Errorf("returned id %d should be >= 1", id2)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err(): %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected 1 row from INSERT RETURNING, got %d (empty result causes Laravel 'Undefined array key 0')", rowCount)
	}
}
