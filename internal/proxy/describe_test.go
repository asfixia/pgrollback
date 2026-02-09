package proxy

import (
	"testing"
)

// Tests in this file assert DescribeRowFieldsForQuery returns non-empty fields for
// INSERT/UPDATE/DELETE ... RETURNING so clients (e.g. Laravel Eloquent) that rely on
// Describe get the correct result shape. Regression: empty RowDescription caused
// "Undefined array key 0" in Laravel's PostgresProcessor::processInsertGetId.

// TestDescribeRowFieldsForQuery_InsertReturningId asserts that the proxy returns a non-empty
// RowDescription for INSERT ... RETURNING "id". This is required so clients that rely on Describe
// (e.g. PHP PDO, Laravel Eloquent) get the correct result shape. If we regress to sending empty
// RowDescription on Describe, Laravel's processInsertGetId gets an empty array and hits
// "Undefined array key 0".
func TestDescribeRowFieldsForQuery_InsertReturningId(t *testing.T) {
	query := `INSERT INTO "p_conab_cafe"."site_user_token" ("user_id", "token", "valid_until") VALUES ($1, $2, $3) RETURNING "id"`
	fields := DescribeRowFieldsForQuery(query)
	if len(fields) == 0 {
		t.Fatal("DescribeRowFieldsForQuery must return at least one field for INSERT RETURNING \"id\" (required for Laravel Eloquent); got 0 fields")
	}
	if len(fields) != 1 {
		t.Errorf("expected 1 field for RETURNING \"id\", got %d", len(fields))
	}
	if name := string(fields[0].Name); name != "id" {
		t.Errorf("first field name = %q, want \"id\"", name)
	}
}

// TestDescribeRowFieldsForQuery_NoReturning asserts that queries without RETURNING get nil (empty description).
func TestDescribeRowFieldsForQuery_NoReturning(t *testing.T) {
	query := `INSERT INTO t (a) VALUES ($1)`
	fields := DescribeRowFieldsForQuery(query)
	if fields != nil {
		t.Errorf("expected nil for INSERT without RETURNING, got %d fields", len(fields))
	}
}

// TestDescribeRowFieldsForQuery_ReturningMultipleColumns asserts RETURNING "id", "name" yields two fields.
func TestDescribeRowFieldsForQuery_ReturningMultipleColumns(t *testing.T) {
	query := `INSERT INTO t (a, b) VALUES ($1, $2) RETURNING "id", "name"`
	fields := DescribeRowFieldsForQuery(query)
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}
	if string(fields[0].Name) != "id" || string(fields[1].Name) != "name" {
		t.Errorf("field names = %q, %q; want \"id\", \"name\"", fields[0].Name, fields[1].Name)
	}
}
