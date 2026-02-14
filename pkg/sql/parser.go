package sql

// ReturningColumn describes a column in a RETURNING clause (name and PostgreSQL type OID).
// Used by both AST (GetReturningColumns) and fallback (ReturningColumnsFallback).
type ReturningColumn struct {
	Name string
	OID  uint32
}

// INT8OID is the PostgreSQL OID for bigint (typical for id columns).
const INT8OID = 20

// TEXTOID is the PostgreSQL OID for text.
const TEXTOID = 25
