package sql

import (
	"testing"
)

func TestReturningColumns(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantNum  int
		wantName string
		wantOID  uint32
	}{
		{
			name:     "INSERT RETURNING quoted id",
			query:    `INSERT INTO t (a) VALUES ($1) RETURNING "id"`,
			wantNum:  1,
			wantName: "id",
			wantOID:  INT8OID,
		},
		{
			name:     "INSERT RETURNING unquoted id",
			query:    `INSERT INTO users (name) VALUES ($1) RETURNING id`,
			wantNum:  1,
			wantName: "id",
			wantOID:  INT8OID,
		},
		{
			name:     "INSERT RETURNING id and name",
			query:    `INSERT INTO t (a, b) VALUES ($1, $2) RETURNING "id", "name"`,
			wantNum:  2,
			wantName: "id",
			wantOID:  INT8OID,
		},
		{
			name:     "UPDATE RETURNING id",
			query:    `UPDATE t SET x = $1 RETURNING "id"`,
			wantNum:  1,
			wantName: "id",
			wantOID:  INT8OID,
		},
		{
			name:     "DELETE RETURNING id",
			query:    `DELETE FROM t WHERE id = $1 RETURNING "id"`,
			wantNum:  1,
			wantName: "id",
			wantOID:  INT8OID,
		},
		{
			name:    "no RETURNING",
			query:   `INSERT INTO t (a) VALUES (1)`,
			wantNum: 0,
		},
		{
			name:    "RETURNING * not supported",
			query:   `INSERT INTO t (a) VALUES (1) RETURNING *`,
			wantNum: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := ReturningColumns(tt.query)
			if len(cols) != tt.wantNum {
				t.Errorf("ReturningColumns() len = %d, want %d", len(cols), tt.wantNum)
				return
			}
			if tt.wantNum > 0 {
				if cols[0].Name != tt.wantName {
					t.Errorf("first column name = %q, want %q", cols[0].Name, tt.wantName)
				}
				if cols[0].OID != tt.wantOID {
					t.Errorf("first column OID = %d, want %d", cols[0].OID, tt.wantOID)
				}
			}
		})
	}
}

func TestReturningColumnsTwoColumns(t *testing.T) {
	cols := ReturningColumns(`INSERT INTO t (a, b) VALUES ($1, $2) RETURNING "id", "name"`)
	if len(cols) != 2 {
		t.Fatalf("want 2 columns, got %d", len(cols))
	}
	if cols[0].Name != "id" || cols[0].OID != INT8OID {
		t.Errorf("col0: name=%q oid=%d, want id and INT8OID", cols[0].Name, cols[0].OID)
	}
	if cols[1].Name != "name" || cols[1].OID != TEXTOID {
		t.Errorf("col1: name=%q oid=%d, want name and TEXTOID", cols[1].Name, cols[1].OID)
	}
}
