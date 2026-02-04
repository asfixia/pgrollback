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
