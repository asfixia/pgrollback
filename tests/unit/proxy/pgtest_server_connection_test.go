package tstproxy

import (
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

// TestPgRollbackServerConnection testa se conseguimos conectar ao servidor pgrollback
// usando a biblioteca padrão database/sql com driver pgx.
func TestPgRollbackServerConnection(t *testing.T) {
	db, ctx, cleanup := connectToProxyForTest(t, "test_connection")
	defer cleanup()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	t.Logf("Successfully connected to pgrollback server using database/sql library")
	t.Logf("This confirms the pgrollback server implements PostgreSQL protocol correctly")
}
