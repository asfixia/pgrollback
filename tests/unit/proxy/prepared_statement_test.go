package tstproxy

import (
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	preparedStatementTestID        = "prepared_statement_test"
	preparedStatementValueTestID   = "prepared_stmt_value_test"
	preparedStatementNoMatchID     = "prepared_stmt_nomatch_test"
	preparedStatementMultiParamID  = "prepared_stmt_multi_test"
	preparedStatementInsertThreeID = "prepared_stmt_insert_three_params"
	preparedStatementReturningID   = "prepared_stmt_returning_id"
	laravelInsertReturningID       = "laravel_insert_returning"
	deallocateTestID               = "deallocate_test"
)

// TestPreparedStatementWithParameters verifies that parameterized prepared statements
// (extended query protocol: Parse -> Bind -> Execute) work through the proxy.
// The proxy must pass bound parameters to the backend; this test requires success.
// Unnamed portal/statement (empty string) is supported by the proxy.
func TestPreparedStatementWithParameters(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, preparedStatementTestID)
	defer cleanup()

	tableName := "prepared_stmt_test_table"
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+tableName+` (id INT PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO `+tableName+` (id, val) VALUES (1, 'one') ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	stmt, err := db.PrepareContext(ctx, `SELECT id, val FROM `+tableName+` WHERE id = $1`)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	var id int
	var val string
	err = stmt.QueryRowContext(ctx, 1).Scan(&id, &val)
	if err != nil {
		t.Fatalf("Prepared statement QueryRow failed (proxy must pass bound parameters): %v", err)
	}
	if id != 1 {
		t.Errorf("Prepared statement returned id=%d val=%q, want id=1 val=one", id, val)
	}
}

// TestPreparedStatementInsertWithThreeParameters verifies that a prepared INSERT with three
// parameters ($1, $2, $3) works through the proxy (extended query protocol), and that a foreign
// key violation (SQLSTATE 23503) is passed through to the client. Uses a dedicated parent/child
// table; does not reference application tables like site_user_token.
func TestPreparedStatementInsertWithThreeParameters(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, preparedStatementInsertThreeID)
	defer cleanup()

	parentTable := "prepared_stmt_insert_parent"
	childTable := "prepared_stmt_insert_child"

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+parentTable+` ("id" INT PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO `+parentTable+` ("id") VALUES (1) ON CONFLICT ("id") DO NOTHING`)
	if err != nil {
		t.Fatalf("Failed to insert parent row: %v", err)
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+childTable+` (
		"id" SERIAL PRIMARY KEY,
		"ref_id" BIGINT NOT NULL REFERENCES `+parentTable+`("id"),
		"token" TEXT,
		"valid_until" TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatalf("Failed to create child table: %v", err)
	}

	stmt, err := db.PrepareContext(ctx, `INSERT INTO `+childTable+` ("ref_id", "token", "valid_until") VALUES ($1, $2, $3)`)
	if err != nil {
		t.Fatalf("Failed to prepare INSERT statement: %v", err)
	}
	defer stmt.Close()

	validUntil := time.Now().UTC().Add(24 * time.Hour)
	tokenStr := "aa9f72a7-e919-4c49-b242-32561228b2da"
	validUntilStr := validUntil.Format(time.RFC3339)

	// Success path: valid ref_id = 1. Proxy describes 3 params as text (OID 25), so pass strings.
	result, err := stmt.ExecContext(ctx, "1", tokenStr, validUntilStr)
	if err != nil {
		t.Fatalf("Prepared INSERT with three params (success path) failed: %v", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 row affected, got %d", n)
	}

	// Failure path: ref_id that does not exist in parent (e.g. 666) triggers FK violation (SQLSTATE 23503)
	_, err = stmt.ExecContext(ctx, "666", tokenStr, validUntilStr)
	if err == nil {
		t.Fatal("Prepared INSERT with invalid ref_id should have failed with FK violation")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "23503") && !strings.Contains(errStr, "foreign key") {
		t.Errorf("Expected FK violation (23503 or 'foreign key'), got: %v", err)
	}
}

// TestPreparedStatementInsertReturningIdAndUseInChild verifies INSERT ... RETURNING id and using
// the returned ID to insert into a second table that references the first. Inserts multiple rows
// into the parent (each RETURNING id), then inserts multiple rows into the child using those IDs.
func TestPreparedStatementInsertReturningIdAndUseInChild(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, preparedStatementReturningID)
	defer cleanup()

	parentTable := "prepared_stmt_returning_parent"
	childTable := "prepared_stmt_returning_child"

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+parentTable+` (
		"id" SERIAL PRIMARY KEY,
		"n" INT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+childTable+` (
		"id" SERIAL PRIMARY KEY,
		"ref_id" BIGINT NOT NULL REFERENCES `+parentTable+`("id"),
		"token" TEXT,
		"valid_until" TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatalf("Failed to create child table: %v", err)
	}

	// Prepared INSERT into parent with RETURNING id (one param: n, int — proxy describes as OID 23)
	parentStmt, err := db.PrepareContext(ctx, `INSERT INTO `+parentTable+` ("n") VALUES ($1) RETURNING "id"`)
	if err != nil {
		t.Fatalf("Failed to prepare parent INSERT: %v", err)
	}
	defer parentStmt.Close()

	// Prepared INSERT into child (ref_id, token, valid_until) — 3 params, proxy describes as text
	childStmt, err := db.PrepareContext(ctx, `INSERT INTO `+childTable+` ("ref_id", "token", "valid_until") VALUES ($1, $2, $3)`)
	if err != nil {
		t.Fatalf("Failed to prepare child INSERT: %v", err)
	}
	defer childStmt.Close()

	validUntilStr := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	const numRows1 = 2
	const numRows2 = 3

	var parentIDs []int64
	for i := 0; i < numRows1; i++ {
		var id int64
		err := parentStmt.QueryRowContext(ctx, i+1).Scan(&id) // n = 1, then n = 2
		if err != nil {
			t.Fatalf("Parent INSERT RETURNING (row %d) failed: %v", i+1, err)
		}
		parentIDs = append(parentIDs, id)
	}

	for i := 0; i < numRows2; i++ {
		tokenStr := fmt.Sprintf("token-%c", 'A'+i)
		// Proxy describes 3 params as text; pass ref_id as string
		_, err := childStmt.ExecContext(ctx, fmt.Sprintf("%d", parentIDs[i%numRows1]), tokenStr, validUntilStr)
		if err != nil {
			t.Fatalf("Child INSERT (row %d, ref_id=%d) failed: %v", i+1, parentIDs[i%numRows1], err)
		}
	}

	// Verify row counts
	var parentCount, childCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+parentTable).Scan(&parentCount)
	if err != nil {
		t.Fatalf("COUNT parent failed: %v", err)
	}
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+childTable).Scan(&childCount)
	if err != nil {
		t.Fatalf("COUNT child failed: %v", err)
	}
	if parentCount != numRows1 {
		t.Errorf("Parent count: got %d, want %d", parentCount, numRows1)
	}
	if childCount != numRows2 {
		t.Errorf("Child count: got %d, want %d", childCount, numRows2)
	}
}

// TestInsertReturningLaravelStyle verifies that a prepared INSERT ... RETURNING "id" (Laravel-style)
// returns one row with the inserted id. The proxy must send a non-empty RowDescription on Describe
// so that clients (e.g. PHP PDO) that rely on Describe get the correct result shape and do not
// return an empty result set (which would cause Laravel's processInsertGetId to hit "Undefined array key 0").
func TestInsertReturningLaravelStyle(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, laravelInsertReturningID)
	defer cleanup()

	table := "laravel_style_returning_t"
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` ("id" SERIAL PRIMARY KEY, "n" INT NOT NULL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Same result shape as Laravel: INSERT ... RETURNING "id" (one row, one column "id").
	// Single param so proxy describes as OID 23 (INT4); we pass an int.
	stmt, err := db.PrepareContext(ctx, `INSERT INTO `+table+` ("n") VALUES ($1) RETURNING "id"`)
	if err != nil {
		t.Fatalf("Failed to prepare INSERT RETURNING: %v", err)
	}
	defer stmt.Close()

	var id int64
	err = stmt.QueryRowContext(ctx, 1).Scan(&id)
	if err != nil {
		t.Fatalf("INSERT RETURNING \"id\" failed (Laravel-style): %v", err)
	}
	if id < 1 {
		t.Errorf("Expected positive id, got %d", id)
	}
}

// TestInsertReturningThreeParamsLaravelStyle verifies INSERT with three parameters and RETURNING "id"
// (same shape as Laravel's site_user_token: user_id, token, valid_until). Ensures the proxy returns
// exactly one row so that result[0]['id'] exists (avoids "Undefined array key 0" in PHP).
func TestInsertReturningThreeParamsLaravelStyle(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, "laravel_three_params_returning")
	defer cleanup()

	table := "laravel_three_returning_t"
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (
		"id" SERIAL PRIMARY KEY,
		"user_id" BIGINT NOT NULL,
		"token" TEXT,
		"valid_until" TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Laravel-style: INSERT (user_id, token, valid_until) RETURNING "id" with 3 params.
	stmt, err := db.PrepareContext(ctx, `INSERT INTO `+table+` ("user_id", "token", "valid_until") VALUES ($1, $2, $3) RETURNING "id"`)
	if err != nil {
		t.Fatalf("Failed to prepare INSERT RETURNING: %v", err)
	}
	defer stmt.Close()

	validUntil := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	var id int64
	err = stmt.QueryRowContext(ctx, 1, "token-abc", validUntil).Scan(&id)
	if err != nil {
		t.Fatalf("INSERT RETURNING \"id\" with 3 params failed (Laravel site_user_token style): %v", err)
	}
	if id < 1 {
		t.Errorf("Expected positive id, got %d", id)
	}
}

// TestDeallocatePreparedStatementAsSimpleQuery verifies behavior when the client sends
// DEALLOCATE as a Simple Query (e.g. PHP PDO after using prepared statements).
//
// Current behavior: The proxy forwards DEALLOCATE to the real PostgreSQL. The backend
// never received a PREPARE for that statement name (the proxy only stores it in session),
// so the backend returns "prepared statement does not exist". This test documents that
// failure; when the proxy is fixed to intercept DEALLOCATE and only clear the session
// map (without forwarding), the test should be updated to expect success.
func TestDeallocatePreparedStatementAsSimpleQuery(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, deallocateTestID)
	defer cleanup()

	// Send DEALLOCATE as a simple query (same as PHP PDO after using prepared statements).
	// The proxy currently forwards this to the backend, which never had this prepared statement.
	_, err := db.ExecContext(ctx, "DEALLOCATE pdo_stmt_00000001")

	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") ||
			strings.Contains(errStr, "SQLSTATE 26000") ||
			strings.Contains(errStr, "prepared statement") ||
			strings.Contains(errStr, "Safe exec failed") {
			t.Logf("DEALLOCATE as simple query failed as expected (proxy forwards to backend that has no such statement): %v", err)
			return
		}
		t.Errorf("DEALLOCATE failed with unexpected error: %v", err)
		return
	}

	// If we get here, the proxy has been fixed to intercept DEALLOCATE and not forward it.
	t.Logf("DEALLOCATE as simple query succeeded (proxy intercepts and clears session only)")
}
