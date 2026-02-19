package postgres_test

import (
	"testing"

	"pgrollback/pkg/postgres"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "public",
			expected: `"public"`,
		},
		{
			name:     "identifier with underscore",
			input:    "pgrollback_table",
			expected: `"pgrollback_table"`,
		},
		{
			name:     "identifier with quotes",
			input:    `schema"name`,
			expected: `"schema""name"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
		{
			name:     "mixed case",
			input:    "PublicSchema",
			expected: `"PublicSchema"`,
		},
		{
			name:     "with spaces",
			input:    "schema name",
			expected: `"schema name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postgres.QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "simple qualified name",
			schema:   "public",
			table:    "pgrollback_table",
			expected: `"public"."pgrollback_table"`,
		},
		{
			name:     "with quotes in names",
			schema:   `schema"name`,
			table:    `table"name`,
			expected: `"schema""name"."table""name"`,
		},
		{
			name:     "mixed case",
			schema:   "PublicSchema",
			table:    "PgRollback_Table",
			expected: `"PublicSchema"."PgRollback_Table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postgres.QuoteQualifiedName(tt.schema, tt.table)
			if result != tt.expected {
				t.Errorf("QuoteQualifiedName(%q, %q) = %q, want %q", tt.schema, tt.table, result, tt.expected)
			}
		})
	}
}
