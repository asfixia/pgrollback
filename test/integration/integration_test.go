package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pgtest/pgtest/internal/config"
	"github.com/pgtest/pgtest/internal/proxy"
)

var pgtestServer *proxy.Server
var pgtestInstance *proxy.PGTest

func TestMain(m *testing.M) {
	cfg, err := config.LoadConfig("")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	pgtestInstance = proxy.NewPGTest(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
	)

	pgtestServer = proxy.NewServer(pgtestInstance)

	go func() {
		if err := pgtestServer.Start(5433); err != nil {
			fmt.Printf("Failed to start server: %v\n", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	code := m.Run()

	pgtestServer.Stop()
	os.Exit(code)
}

func getPGTestDSN(testID string) string {
	return fmt.Sprintf("host=localhost port=5433 user=postgres password=postgres dbname=postgres sslmode=disable application_name=pgtest_%s", testID)
}

func getPostgresDSN() string {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	pass := os.Getenv("POSTGRES_PASSWORD")
	if pass == "" {
		pass = "postgres"
	}
	db := os.Getenv("POSTGRES_DB")
	if db == "" {
		db = "postgres"
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)
}

func TestProtectionAgainstAccidentalCommit(t *testing.T) {
	testID := "test_commit_protection"
	pgtestDSN := getPGTestDSN(testID)

	db, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_commit_protection (id SERIAL PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_commit_protection (value) VALUES ('test_value')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_commit_protection").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	_, err = db.Exec("COMMIT")
	if err != nil {
		t.Logf("COMMIT was blocked (expected): %v", err)
	} else {
		t.Log("COMMIT executed (should be blocked)")
	}

	postgresDSN := getPostgresDSN()
	postgresDB, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer postgresDB.Close()

	var countInPostgres int
	err = postgresDB.QueryRow("SELECT COUNT(*) FROM test_commit_protection").Scan(&countInPostgres)
	if err == nil {
		if countInPostgres > 0 {
			t.Errorf("CRITICAL: Data was committed to PostgreSQL! Count = %d (should be 0)", countInPostgres)
		} else {
			t.Log("SUCCESS: Data was NOT committed to PostgreSQL")
		}
	}

	_, err = db.Exec("pgtest rollback " + testID)
	if err != nil {
		t.Logf("Rollback via pgtest command: %v", err)
	}

	err = postgresDB.QueryRow("SELECT COUNT(*) FROM test_commit_protection").Scan(&countInPostgres)
	if err == nil {
		if countInPostgres > 0 {
			t.Errorf("CRITICAL: Data still exists after rollback! Count = %d (should be 0)", countInPostgres)
		}
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS test_commit_protection")
}

func TestProtectionAgainstAccidentalRollback(t *testing.T) {
	testID := "test_rollback_protection"
	pgtestDSN := getPGTestDSN(testID)

	db, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_rollback_protection (id SERIAL PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_rollback_protection (value) VALUES ('test_value')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_rollback_protection").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	_, err = db.Exec("ROLLBACK")
	if err != nil {
		t.Logf("ROLLBACK was blocked (expected): %v", err)
	} else {
		t.Log("ROLLBACK executed (should be blocked)")
	}

	count = 0
	err = db.QueryRow("SELECT COUNT(*) FROM test_rollback_protection").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query after ROLLBACK attempt: %v", err)
	}
	if count != 1 {
		t.Errorf("CRITICAL: Data was rolled back! Count = %d (should be 1)", count)
	} else {
		t.Log("SUCCESS: Data still exists after ROLLBACK attempt")
	}

	_, err = db.Exec("pgtest rollback " + testID)
	if err != nil {
		t.Logf("Rollback via pgtest command: %v", err)
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS test_rollback_protection")
}

func TestTransactionSharing(t *testing.T) {
	testID := "test_sharing"
	pgtestDSN := getPGTestDSN(testID)

	db1, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db2.Close()

	_, err = db1.Exec("CREATE TABLE IF NOT EXISTS test_sharing (id SERIAL PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db1.Exec("INSERT INTO test_sharing (value) VALUES ('from_db1')")
	if err != nil {
		t.Fatalf("Failed to insert from db1: %v", err)
	}

	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM test_sharing").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query from db2: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row visible in db2, got %d", count)
	} else {
		t.Log("SUCCESS: Transaction is shared between connections")
	}

	_, err = db1.Exec("pgtest rollback " + testID)
	if err != nil {
		t.Logf("Rollback: %v", err)
	}

	_, _ = db1.Exec("DROP TABLE IF EXISTS test_sharing")
}

func TestIsolationBetweenTestIDs(t *testing.T) {
	testID1 := "test_isolation_1"
	testID2 := "test_isolation_2"
	pgtestDSN1 := getPGTestDSN(testID1)
	pgtestDSN2 := getPGTestDSN(testID2)

	db1, err := sql.Open("pgx", pgtestDSN1)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open("pgx", pgtestDSN2)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db2.Close()

	_, err = db1.Exec("CREATE TABLE IF NOT EXISTS test_isolation (id SERIAL PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db1.Exec("INSERT INTO test_isolation (value) VALUES ('from_test1')")
	if err != nil {
		t.Fatalf("Failed to insert from test1: %v", err)
	}

	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM test_isolation").Scan(&count)
	if err == nil {
		if count > 0 {
			t.Errorf("CRITICAL: Test-ID isolation broken! Count = %d (should be 0)", count)
		} else {
			t.Log("SUCCESS: Test-IDs are isolated")
		}
	}

	_, err = db1.Exec("pgtest rollback " + testID1)
	if err != nil {
		t.Logf("Rollback test1: %v", err)
	}

	_, err = db2.Exec("pgtest rollback " + testID2)
	if err != nil {
		t.Logf("Rollback test2: %v", err)
	}

	_, _ = db1.Exec("DROP TABLE IF EXISTS test_isolation")
}

func TestBeginToSavepointConversion(t *testing.T) {
	testID := "test_savepoint"
	pgtestDSN := getPGTestDSN(testID)

	db, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_savepoint (id SERIAL PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_savepoint (value) VALUES ('before_begin')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec("BEGIN")
	if err != nil {
		t.Fatalf("Failed to execute BEGIN: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_savepoint (value) VALUES ('after_begin')")
	if err != nil {
		t.Fatalf("Failed to insert after BEGIN: %v", err)
	}

	_, err = db.Exec("COMMIT")
	if err != nil {
		t.Fatalf("Failed to execute COMMIT: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_savepoint").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	_, err = db.Exec("pgtest rollback " + testID)
	if err != nil {
		t.Logf("Rollback: %v", err)
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS test_savepoint")
}

func TestPGTestCommands(t *testing.T) {
	testID := "test_commands"
	pgtestDSN := getPGTestDSN(testID)

	db, err := sql.Open("pgx", pgtestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("pgtest begin " + testID)
	if err != nil {
		t.Logf("pgtest begin: %v", err)
	}

	var testIDCol string
	var active bool
	var level int
	var createdAt string

	err = db.QueryRow("pgtest status " + testID).Scan(&testIDCol, &active, &level, &createdAt)
	if err != nil {
		t.Logf("pgtest status: %v", err)
	} else {
		if testIDCol != testID {
			t.Errorf("Status test_id = %v, want %v", testIDCol, testID)
		}
		if !active {
			t.Error("Status active should be true")
		}
	}

	_, err = db.Exec("pgtest rollback " + testID)
	if err != nil {
		t.Logf("pgtest rollback: %v", err)
	}
}
