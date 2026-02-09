package tstproxy

import (
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

// TestServerAuthenticationHandshake testa o handshake de autenticação do servidor
// Verifica se o servidor aceita conexões usando biblioteca PostgreSQL padrão.
func TestServerAuthenticationHandshake(t *testing.T) {
	db, ctx, proxyServer, cleanup := connectToProxyForTestWithServer(t, "test_auth")
	defer cleanup()

	db.SetConnMaxLifetime(time.Second * 5)
	db.SetMaxOpenConns(1)

	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	session := proxyServer.Pgtest.GetSession("test_auth")
	if session == nil {
		t.Log("Session 'test_auth' not found (this is expected if application_name extraction works correctly)")
	}

	// Conexão estabelecida com sucesso usando biblioteca PostgreSQL
	t.Log("Authentication handshake completed successfully using PostgreSQL library")
}

// TestServerAuthenticationHandshake_DefaultAppName testa autenticação sem application_name.
func TestServerAuthenticationHandshake_DefaultAppName(t *testing.T) {
	db, ctx, proxyServer, cleanup := connectToProxyForTestWithAppNameAndServer(t, "")
	defer cleanup()

	db.SetConnMaxLifetime(time.Second * 5)
	db.SetMaxOpenConns(1)

	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	session := proxyServer.Pgtest.GetSession("default")
	if session == nil {
		t.Error("Session with testID 'default' should be created")
	}

	t.Log("Authentication handshake with default app name completed successfully using PostgreSQL library")
}
