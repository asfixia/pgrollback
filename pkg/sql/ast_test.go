//go:build cgo
// +build cgo

package sql

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func mustParse(t *testing.T, sql string) []*pg_query.RawStmt {
	t.Helper()
	stmts, err := ParseStatements(sql)
	if err != nil {
		t.Fatalf("ParseStatements(%q): %v", sql, err)
	}
	return stmts
}

func firstStmt(t *testing.T, sql string) *pg_query.Node {
	stmts := mustParse(t, sql)
	if len(stmts) == 0 {
		t.Fatalf("expected at least one statement: %q", sql)
	}
	return stmts[0].Stmt
}

func TestParseStatements(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		stmts := mustParse(t, "SELECT 1")
		if len(stmts) != 1 {
			t.Fatalf("expected 1 statement, got %d", len(stmts))
		}
	})
	t.Run("multi", func(t *testing.T) {
		stmts := mustParse(t, "SELECT 1; SELECT 2")
		if len(stmts) != 2 {
			t.Fatalf("expected 2 statements, got %d", len(stmts))
		}
	})
	t.Run("with_comments", func(t *testing.T) {
		stmts := mustParse(t, "SELECT 1; -- comment\nSELECT 2")
		if len(stmts) != 2 {
			t.Fatalf("expected 2 statements, got %d", len(stmts))
		}
	})
	t.Run("quoted_semicolon", func(t *testing.T) {
		stmts := mustParse(t, `SELECT 'a;b'`)
		if len(stmts) != 1 {
			t.Fatalf("expected 1 statement (semicolon in string), got %d", len(stmts))
		}
	})
}

func TestClassifyStatement(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"SELECT 1", "SELECT"},
		{"INSERT INTO t (a) VALUES (1)", "INSERT"},
		{"UPDATE t SET a = 1", "UPDATE"},
		{"DELETE FROM t", "DELETE"},
		{"BEGIN", "BEGIN"},
		{"START TRANSACTION", "BEGIN"},
		{"COMMIT", "COMMIT"},
		{"ROLLBACK", "ROLLBACK"},
		{"SAVEPOINT sp1", "SAVEPOINT"},
		{"RELEASE SAVEPOINT sp1", "RELEASE"},
		{"ROLLBACK TO SAVEPOINT sp1", "ROLLBACK"},
		{"DEALLOCATE x", "DEALLOCATE"},
		{"DEALLOCATE ALL", "DEALLOCATE"},
		{"SET client_encoding = 'UTF8'", "SET"},
		{"CREATE TABLE t (id int)", "CREATE"},
		{"DROP TABLE t", "DROP"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			stmt := firstStmt(t, tt.sql)
			got := ClassifyStatement(stmt)
			if got != tt.want {
				t.Errorf("ClassifyStatement(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

func TestGetReturningColumns(t *testing.T) {
	t.Run("insert_returning_id", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a) VALUES (1) RETURNING "id"`)
		cols := GetReturningColumns(stmt)
		if len(cols) != 1 || cols[0].Name != "id" {
			t.Errorf("got %v", cols)
		}
	})
	t.Run("insert_returning_id_name", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a,b) VALUES (1,2) RETURNING id, name`)
		cols := GetReturningColumns(stmt)
		if len(cols) != 2 {
			t.Errorf("expected 2 columns, got %v", cols)
		}
	})
	t.Run("returning_star", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a) VALUES (1) RETURNING *`)
		cols := GetReturningColumns(stmt)
		if cols != nil {
			t.Errorf("RETURNING * should return nil for Describe, got %v", cols)
		}
	})
	t.Run("no_returning", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a) VALUES (1)`)
		cols := GetReturningColumns(stmt)
		if cols != nil {
			t.Errorf("expected nil, got %v", cols)
		}
	})
	t.Run("update_returning", func(t *testing.T) {
		stmt := firstStmt(t, `UPDATE t SET a = 1 RETURNING id`)
		cols := GetReturningColumns(stmt)
		if len(cols) != 1 || cols[0].Name != "id" {
			t.Errorf("got %v", cols)
		}
	})
	t.Run("delete_returning", func(t *testing.T) {
		stmt := firstStmt(t, `DELETE FROM t RETURNING id`)
		cols := GetReturningColumns(stmt)
		if len(cols) != 1 || cols[0].Name != "id" {
			t.Errorf("got %v", cols)
		}
	})
}

func TestStmtReturnsResultSet(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT 1")
		if !StmtReturnsResultSet(stmt) {
			t.Error("SELECT should return result set")
		}
	})
	t.Run("insert_returning", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a) VALUES (1) RETURNING id`)
		if !StmtReturnsResultSet(stmt) {
			t.Error("INSERT RETURNING should return result set")
		}
	})
	t.Run("insert_no_returning", func(t *testing.T) {
		stmt := firstStmt(t, `INSERT INTO t (a) VALUES (1)`)
		if StmtReturnsResultSet(stmt) {
			t.Error("INSERT without RETURNING should not return result set")
		}
	})
}

func TestParseDeallocate(t *testing.T) {
	t.Run("deallocate_name", func(t *testing.T) {
		stmt := firstStmt(t, "DEALLOCATE stmt_name")
		name, isAll, ok := ParseDeallocate(stmt)
		if !ok || isAll || name != "stmt_name" {
			t.Errorf("got name=%q isAll=%v ok=%v", name, isAll, ok)
		}
	})
	t.Run("deallocate_all", func(t *testing.T) {
		stmt := firstStmt(t, "DEALLOCATE ALL")
		name, isAll, ok := ParseDeallocate(stmt)
		if !ok || !isAll || name != "" {
			t.Errorf("got name=%q isAll=%v ok=%v", name, isAll, ok)
		}
	})
	t.Run("deallocate_comment_all", func(t *testing.T) {
		stmt := firstStmt(t, "DEALLOCATE /**/ ALL")
		_, isAll, ok := ParseDeallocate(stmt)
		if !ok || !isAll {
			t.Errorf("DEALLOCATE /**/ ALL: got isAll=%v ok=%v", isAll, ok)
		}
	})
	t.Run("not_deallocate", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT 1")
		_, _, ok := ParseDeallocate(stmt)
		if ok {
			t.Error("SELECT should not be DEALLOCATE")
		}
	})
}

func TestMaxParamIndex(t *testing.T) {
	t.Run("params_1_2_1", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT $1, $2, $1")
		got := MaxParamIndex(stmt)
		if got != 2 {
			t.Errorf("MaxParamIndex = %d, want 2", got)
		}
	})
	t.Run("no_params", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT 1")
		got := MaxParamIndex(stmt)
		if got != 0 {
			t.Errorf("MaxParamIndex = %d, want 0", got)
		}
	})
}

func TestSubstituteParams(t *testing.T) {
	t.Run("two_params", func(t *testing.T) {
		got := SubstituteParams("SELECT $1, $2", []any{10, "foo"}, "")
		want := "SELECT 10, 'foo'"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
	t.Run("with_conn_label", func(t *testing.T) {
		got := SubstituteParams("SELECT $1", []any{1}, "conn:127.0.0.1:52586")
		if got != "[conn:127.0.0.1:52586] SELECT 1" {
			t.Errorf("got %q", got)
		}
	})
}

func TestTransactionDetection(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		stmt := firstStmt(t, "BEGIN")
		if !IsTransactionBegin(stmt) {
			t.Error("expected BEGIN")
		}
	})
	t.Run("commit", func(t *testing.T) {
		stmt := firstStmt(t, "COMMIT")
		if !IsTransactionCommit(stmt) {
			t.Error("expected COMMIT")
		}
	})
	t.Run("rollback", func(t *testing.T) {
		stmt := firstStmt(t, "ROLLBACK")
		if !IsTransactionRollback(stmt) {
			t.Error("expected ROLLBACK")
		}
	})
	t.Run("savepoint", func(t *testing.T) {
		stmt := firstStmt(t, "SAVEPOINT sp1")
		if !IsSavepoint(stmt) {
			t.Error("expected SAVEPOINT")
		}
	})
	t.Run("release_savepoint", func(t *testing.T) {
		stmt := firstStmt(t, "RELEASE SAVEPOINT sp1")
		if !IsReleaseSavepoint(stmt) {
			t.Error("expected RELEASE SAVEPOINT")
		}
	})
	t.Run("rollback_to_savepoint", func(t *testing.T) {
		stmt := firstStmt(t, "ROLLBACK TO SAVEPOINT sp1")
		if !IsRollbackToSavepoint(stmt) {
			t.Error("expected ROLLBACK TO SAVEPOINT")
		}
	})
}

func TestGetSavepointName(t *testing.T) {
	t.Run("savepoint", func(t *testing.T) {
		stmt := firstStmt(t, "SAVEPOINT sp1")
		got := GetSavepointName(stmt)
		if got != "sp1" {
			t.Errorf("got %q, want sp1", got)
		}
	})
	t.Run("release", func(t *testing.T) {
		stmt := firstStmt(t, "RELEASE SAVEPOINT my_sp")
		got := GetSavepointName(stmt)
		if got != "my_sp" {
			t.Errorf("got %q, want my_sp", got)
		}
	})
	t.Run("rollback_to", func(t *testing.T) {
		stmt := firstStmt(t, "ROLLBACK TO SAVEPOINT x")
		got := GetSavepointName(stmt)
		if got != "x" {
			t.Errorf("got %q, want x", got)
		}
	})
}

func TestStmtCommandTag(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT 1")
		if got := StmtCommandTag(stmt); got != "SELECT" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("insert", func(t *testing.T) {
		stmt := firstStmt(t, "INSERT INTO t (a) VALUES (1)")
		if got := StmtCommandTag(stmt); got != "INSERT 0 1" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("update", func(t *testing.T) {
		stmt := firstStmt(t, "UPDATE t SET a = 1")
		if got := StmtCommandTag(stmt); got != "UPDATE 0" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("delete", func(t *testing.T) {
		stmt := firstStmt(t, "DELETE FROM t")
		if got := StmtCommandTag(stmt); got != "DELETE 0" {
			t.Errorf("got %q", got)
		}
	})
}

func TestIsDeallocateNoise(t *testing.T) {
	t.Run("deallocate", func(t *testing.T) {
		stmt := firstStmt(t, "DEALLOCATE x")
		if !IsDeallocateNoise(stmt) {
			t.Error("DEALLOCATE should be noise")
		}
	})
	t.Run("select", func(t *testing.T) {
		stmt := firstStmt(t, "SELECT 1")
		if IsDeallocateNoise(stmt) {
			t.Error("SELECT should not be noise")
		}
	})
}
