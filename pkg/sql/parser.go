package sql

import (
	"strings"
)

// SplitCommands divide uma query SQL em comandos individuais separados por ponto e vírgula.
// Respeita strings literais (aspas simples e duplas) para não dividir comandos incorretamente.
func SplitCommands(query string) []string {
	var commands []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(query); i++ {
		char := query[i]

		switch char {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			current.WriteByte(char)
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			current.WriteByte(char)
		case ';':
			if !inSingleQuote && !inDoubleQuote {
				cmd := strings.TrimSpace(current.String())
				if cmd != "" {
					commands = append(commands, cmd)
				}
				current.Reset()
			} else {
				current.WriteByte(char)
			}
		default:
			current.WriteByte(char)
		}
	}

	cmd := strings.TrimSpace(current.String())
	if cmd != "" {
		commands = append(commands, cmd)
	}

	return commands
}

// CommandInfo contém informações sobre um comando SQL
type CommandInfo struct {
	Type string // SELECT, SET, INSERT, UPDATE, DELETE, etc.
	Tag  string // Tag para CommandComplete
}

// AnalyzeCommand analisa um comando SQL e retorna suas informações
func AnalyzeCommand(cmd string) CommandInfo {
	cmdUpper := strings.ToUpper(strings.TrimSpace(cmd))

	commandTypes := []struct {
		prefix string
		tag    string
	}{
		{"SELECT", "SELECT"},
		{"INSERT", "INSERT"},
		{"UPDATE", "UPDATE"},
		{"DELETE", "DELETE"},
		{"CREATE", "CREATE"},
		{"DROP", "DROP"},
		{"ALTER", "ALTER"},
		{"SET", "SET"},
		{"SAVEPOINT", "SAVEPOINT"},
		{"RELEASE", "RELEASE"},
		{"ROLLBACK", "ROLLBACK"},
	}

	for _, commandType := range commandTypes {
		if strings.HasPrefix(cmdUpper, commandType.prefix) {
			return CommandInfo{Type: commandType.prefix, Tag: commandType.tag}
		}
	}

	return CommandInfo{Type: "OTHER", Tag: "OK"}
}

// IsSelect verifica se um comando é SELECT
func IsSelect(cmd string) bool {
	return AnalyzeCommand(cmd).Type == "SELECT"
}

// ReturnsResultSet returns true if the command returns a result set (rows), e.g. SELECT or INSERT/UPDATE/DELETE ... RETURNING.
func ReturnsResultSet(cmd string) bool {
	if IsSelect(cmd) {
		return true
	}
	cmdUpper := strings.ToUpper(strings.TrimSpace(cmd))
	if strings.HasPrefix(cmdUpper, "INSERT") || strings.HasPrefix(cmdUpper, "UPDATE") || strings.HasPrefix(cmdUpper, "DELETE") {
		return strings.Contains(cmdUpper, "RETURNING")
	}
	return false
}

// ReturningColumn describes a column in a RETURNING clause (name and PostgreSQL type OID).
type ReturningColumn struct {
	Name string
	OID  uint32
}

// INT8OID is the PostgreSQL OID for bigint (typical for id columns).
const INT8OID = 20

// TEXTOID is the PostgreSQL OID for text.
const TEXTOID = 25

// ReturningColumns parses the RETURNING clause of INSERT/UPDATE/DELETE and returns column names and OIDs.
// For use when building RowDescription for Describe (Portal/Statement) so clients like PHP see the correct result shape.
// Returns nil if the query has no RETURNING or we cannot parse it (e.g. RETURNING *). Laravel uses explicit column names.
func ReturningColumns(query string) []ReturningColumn {
	idx := strings.Index(strings.ToUpper(query), "RETURNING")
	if idx < 0 {
		return nil
	}
	afterReturning := strings.TrimSpace(query[idx+len("RETURNING"):])
	if afterReturning == "" {
		return nil
	}
	// Trim to end of first statement (semicolon outside quotes)
	afterReturning = trimToEndOfStatement(afterReturning)
	if afterReturning == "" {
		return nil
	}
	// RETURNING * not supported for Describe; Laravel uses explicit columns.
	if strings.TrimSpace(afterReturning) == "*" {
		return nil
	}
	var cols []ReturningColumn
	// Parse comma-separated: "id", "name" or id, name. Allow double-quoted identifiers.
	for len(afterReturning) > 0 {
		afterReturning = strings.TrimSpace(afterReturning)
		if afterReturning == "" {
			break
		}
		var name string
		if strings.HasPrefix(afterReturning, `"`) {
			end := findUnescapedQuote(afterReturning[1:], '"')
			if end < 0 {
				break
			}
			name = afterReturning[1 : 1+end]
			afterReturning = strings.TrimSpace(afterReturning[1+end+1:])
		} else {
			// Unquoted identifier: run until comma or end
			i := 0
			for i < len(afterReturning) && afterReturning[i] != ',' && afterReturning[i] != ';' {
				i++
			}
			name = strings.TrimSpace(afterReturning[:i])
			afterReturning = afterReturning[i:]
		}
		if name == "" {
			break
		}
		oid := uint32(TEXTOID)
		if strings.EqualFold(name, "id") {
			oid = INT8OID
		}
		cols = append(cols, ReturningColumn{Name: name, OID: oid})
		if len(afterReturning) > 0 && afterReturning[0] == ',' {
			afterReturning = afterReturning[1:]
		} else {
			break
		}
	}
	return cols
}

// trimToEndOfStatement returns the string up to (but not including) the first semicolon that is outside single/double quotes.
func trimToEndOfStatement(s string) string {
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case ';':
			if !inSingle && !inDouble {
				return strings.TrimSpace(s[:i])
			}
		}
	}
	return strings.TrimSpace(s)
}

// findUnescapedQuote returns the index of the first unescaped quote (same as opener) in s, or -1.
func findUnescapedQuote(s string, quote byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == quote {
			// In PostgreSQL identifiers, "" inside "..." is an escaped quote
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i
		}
	}
	return -1
}

// GetCommandTag retorna a tag apropriada para CommandComplete baseado no comando
func GetCommandTag(query string) string {
	info := AnalyzeCommand(query)
	
	// Tags especiais para alguns comandos
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(queryUpper, "INSERT") {
		return "INSERT 0 1"
	} else if strings.HasPrefix(queryUpper, "UPDATE") {
		return "UPDATE 0"
	} else if strings.HasPrefix(queryUpper, "DELETE") {
		return "DELETE 0"
	}
	
	return info.Tag
}
