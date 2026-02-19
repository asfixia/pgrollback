package tstproxy

import (
	"context"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

// TestSelect1AsUm testa conexão ao pgrollback usando database/sql e executa SELECT 1 as um.
func TestSelect1AsUm(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "select_test")
	defer cleanup()

	db.SetConnMaxLifetime(30 * time.Second)
	db.SetMaxOpenConns(1)

	queryTimeout := getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second)
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	var result int
	err := db.QueryRowContext(queryCtx, "SELECT 1 as um").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute SELECT 1 as um: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected result to be 1, got %d", result)
	}

	t.Logf("Successfully executed SELECT 1 as um and got result: %d", result)
	t.Logf("This confirms the pgrollback server correctly handles SELECT queries with column aliases")
}

// TestSelectSiteUser runs SELECT * FROM p_conab_cafe.site_user LIMIT 10 through the proxy,
// asserts row count is 10, discovers how many fields are returned, and asserts at least two different column types.
func TestSelectSiteUser(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}

	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	db, ctx, cleanup := connectToProxyForTest(t, "select_site_user")
	defer cleanup()

	queryTimeout := getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second)
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	rows, err := db.QueryContext(queryCtx, `SELECT * FROM "p_conab_cafe"."site_user" LIMIT 10`)
	if err != nil {
		t.Fatalf("Failed to execute site_user SELECT: %v", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}
	fieldCount := len(columnTypes)
	t.Logf("Query returns %d fields (columns)", fieldCount)

	typeNames := make(map[string]struct{})
	for _, ct := range columnTypes {
		typeNames[ct.DatabaseTypeName()] = struct{}{}
	}
	uniqueTypeCount := len(typeNames)
	if uniqueTypeCount < 2 {
		t.Errorf("expected at least 2 different column types, got %d: %v", uniqueTypeCount, typeNames)
	}
	t.Logf("Column types present: %v", typeNames)

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	if rowCount != 10 {
		t.Errorf("expected row count 10, got %d", rowCount)
	}
	t.Logf("Row count: %d", rowCount)
}
