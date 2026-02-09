package tstproxy

import (
	"context"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

// TestPGAdminConnectionNoTimeout testa se o pgAdmin consegue conectar sem timeout.
func TestPGAdminConnectionNoTimeout(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}

	db, ctx, cleanup := connectToProxyForTestWithAppName(t, "")
	defer cleanup()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	pingTimeout := getOrDefault(cfg.Test.PingTimeout.Duration, 3*time.Second)
	startTime := time.Now()
	pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
	err := db.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		elapsed := time.Since(startTime)
		t.Fatalf("Failed to ping pgtest server (elapsed: %v): %v", elapsed, err)
	}
	elapsed := time.Since(startTime)
	t.Logf("Successfully pinged pgtest server in %v (timeout was %v)", elapsed, pingTimeout)

	queryTimeout := getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second)
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	var result int
	err = db.QueryRowContext(queryCtx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query after ping: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	t.Logf("Successfully connected to pgtest server and executed query without timeout")
}
