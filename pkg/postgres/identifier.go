package postgres

import (
	"strings"
)

// QuoteIdentifier escapa um identificador PostgreSQL (schema, tabela, coluna, etc.)
// Adiciona aspas duplas e escapa aspas duplas internas duplicando-as.
// Exemplo: "public" → `"public"`, `schema name` → `"schema name"`
func QuoteIdentifier(identifier string) string {
	if identifier == "" {
		return `""`
	}

	// Escapa aspas duplas internas duplicando-as
	escaped := strings.ReplaceAll(identifier, `"`, `""`)

	// Adiciona aspas duplas ao redor
	return `"` + escaped + `"`
}

// QuoteQualifiedName escapa um nome qualificado (schema.table)
// Exemplo: "public"."pgrollback_table" → `"public"."pgrollback_table"`
func QuoteQualifiedName(schema, table string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}
