// Package sql provides SQL parsing and AST helpers using pg_query_go/v5.
// AST-based functions live here; consumers should use these instead of string-based parsing where applicable.
package sql

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// ParseStatements parses SQL and returns one RawStmt per statement (replaces SplitCommands).
func ParseStatements(sql string) ([]*pg_query.RawStmt, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	if tree == nil || tree.Stmts == nil {
		return nil, nil
	}
	return tree.Stmts, nil
}

// CommandStringFromRaw returns the SQL substring for a single RawStmt (PG uses 1-based StmtLocation).
// Returns trimmed command, or empty string if location/len are invalid.
func CommandStringFromRaw(query string, raw *pg_query.RawStmt) string {
	if raw == nil {
		return ""
	}
	loc := int(raw.GetStmtLocation())
	length := int(raw.GetStmtLen())
	if loc < 1 || length <= 0 {
		return ""
	}
	start := loc - 1
	end := start + length
	if end > len(query) {
		end = len(query)
	}
	if start >= len(query) {
		return ""
	}
	return strings.TrimSpace(query[start:end])
}

// ClassifyStatement returns the statement kind: SELECT, INSERT, UPDATE, DELETE, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DEALLOCATE, SET, CREATE, DROP, OTHER.
func ClassifyStatement(stmt *pg_query.Node) string {
	if stmt == nil {
		return "OTHER"
	}
	if stmt.GetSelectStmt() != nil {
		return "SELECT"
	}
	if stmt.GetInsertStmt() != nil {
		return "INSERT"
	}
	if stmt.GetUpdateStmt() != nil {
		return "UPDATE"
	}
	if stmt.GetDeleteStmt() != nil {
		return "DELETE"
	}
	if t := stmt.GetTransactionStmt(); t != nil {
		switch t.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
			return "BEGIN"
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			return "COMMIT"
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			return "ROLLBACK"
		case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
			return "SAVEPOINT"
		case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
			return "RELEASE"
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
			return "ROLLBACK" // ROLLBACK TO SAVEPOINT
		default:
			return "OTHER"
		}
	}
	if stmt.GetVariableSetStmt() != nil {
		return "SET"
	}
	if stmt.GetCreateStmt() != nil {
		return "CREATE"
	}
	if stmt.GetDropStmt() != nil {
		return "DROP"
	}
	if stmt.GetDeallocateStmt() != nil {
		return "DEALLOCATE"
	}
	return "OTHER"
}

// returningColumnName extracts a single column name from a RETURNING list item (Node).
// Returns "" if the item is RETURNING * or an expression we can't describe (e.g. expr AS alias).
func returningColumnName(n *pg_query.Node) string {
	if n == nil {
		return ""
	}
	rt := n.GetResTarget()
	if rt != nil {
		if name := rt.GetName(); name != "" {
			return name
		}
		// ResTarget with empty name: val is often ColumnRef (e.g. RETURNING id)
		if val := rt.GetVal(); val != nil {
			if cr := val.GetColumnRef(); cr != nil {
				return columnRefName(cr)
			}
		}
		return ""
	}
	// Direct ColumnRef in RETURNING list (some parses)
	if cr := n.GetColumnRef(); cr != nil {
		return columnRefName(cr)
	}
	return ""
}

// columnRefName returns the single column name from a ColumnRef (first field as string), or "".
func columnRefName(cr *pg_query.ColumnRef) string {
	fields := cr.GetFields()
	if len(fields) == 0 {
		return ""
	}
	// Simple column: one field, String node with the identifier
	first := fields[0]
	if first == nil {
		return ""
	}
	if s := first.GetString_(); s != nil {
		return s.GetSval()
	}
	return ""
}

// GetReturningColumns extracts RETURNING column names from INSERT/UPDATE/DELETE stmt (for RowDescription).
// Returns nil for RETURNING * or when no RETURNING clause.
func GetReturningColumns(stmt *pg_query.Node) []ReturningColumn {
	if stmt == nil {
		return nil
	}
	var list []*pg_query.Node
	if s := stmt.GetInsertStmt(); s != nil {
		list = s.GetReturningList()
	}
	if s := stmt.GetUpdateStmt(); s != nil {
		list = s.GetReturningList()
	}
	if s := stmt.GetDeleteStmt(); s != nil {
		list = s.GetReturningList()
	}
	if list == nil || len(list) == 0 {
		return nil
	}
	var cols []ReturningColumn
	for _, n := range list {
		name := returningColumnName(n)
		if name == "" {
			// RETURNING * or unsupported expression
			return nil
		}
		oid := uint32(TEXTOID)
		if strings.EqualFold(name, "id") {
			oid = INT8OID
		}
		cols = append(cols, ReturningColumn{Name: name, OID: oid})
	}
	return cols
}

// StmtReturnsResultSet is true for SELECT or for INSERT/UPDATE/DELETE with RETURNING (AST-based).
func StmtReturnsResultSet(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	if stmt.GetSelectStmt() != nil {
		return true
	}
	return len(GetReturningColumns(stmt)) > 0
}

// ParseDeallocate returns (name, isAll, true) for a DEALLOCATE statement; otherwise ("", false, false).
func ParseDeallocate(stmt *pg_query.Node) (name string, isAll bool, ok bool) {
	if stmt == nil {
		return "", false, false
	}
	d := stmt.GetDeallocateStmt()
	if d == nil {
		return "", false, false
	}
	n := d.GetName()
	if n == "" {
		return "", true, true
	}
	return n, false, true
}

// paramRefPos holds location (1-based in PG) and param number for substitution.
type paramRefPos struct {
	location int
	number   int32
}

// collectParamRefs appends all ParamRef (location, number) from the AST into out.
func collectParamRefs(node *pg_query.Node, out *[]paramRefPos) {
	walkNodeTree(node, func(n *pg_query.Node) {
		if n != nil {
			if pr := n.GetParamRef(); pr != nil {
				*out = append(*out, paramRefPos{location: int(pr.GetLocation()), number: pr.GetNumber()})
			}
		}
	})
}

// walkNodeTree visits node and every descendant *pg_query.Node via reflection (oneof + struct fields).
func walkNodeTree(node *pg_query.Node, visit func(*pg_query.Node)) {
	if node == nil {
		return
	}
	visit(node)
	// Unwrap Node oneof to get inner wrapper (e.g. *Node_SelectStmt).
	nodeVal := reflect.ValueOf(node).Elem()
	oneofField := nodeVal.FieldByName("Node")
	if !oneofField.IsValid() || oneofField.IsNil() {
		return
	}
	oneof := oneofField.Interface()
	walkValue(oneof, visit)
}

func walkValue(val interface{}, visit func(*pg_query.Node)) {
	if val == nil {
		return
	}
	v := reflect.ValueOf(val)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	nodeType := reflect.TypeOf((*pg_query.Node)(nil))
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if !f.CanInterface() {
			continue
		}
		switch f.Kind() {
		case reflect.Ptr:
			if f.IsNil() {
				continue
			}
			if f.Type().AssignableTo(nodeType) {
				if n, ok := f.Interface().(*pg_query.Node); ok {
					walkNodeTree(n, visit)
				}
			} else if f.Elem().Kind() == reflect.Struct {
				walkValue(f.Interface(), visit)
			}
		case reflect.Slice:
			for j := 0; j < f.Len(); j++ {
				item := f.Index(j)
				if item.Kind() == reflect.Ptr && !item.IsNil() {
					if item.Type().AssignableTo(nodeType) {
						if n, ok := item.Interface().(*pg_query.Node); ok {
							walkNodeTree(n, visit)
						}
					} else if item.Elem().Kind() == reflect.Struct {
						walkValue(item.Interface(), visit)
					}
				}
			}
		}
	}
}

// MaxParamIndex returns the highest $n parameter index in the statement (1-based); 0 if none.
func MaxParamIndex(stmt *pg_query.Node) int {
	if stmt == nil {
		return 0
	}
	var refs []paramRefPos
	collectParamRefs(stmt, &refs)
	max := 0
	for _, r := range refs {
		if int(r.number) > max {
			max = int(r.number)
		}
	}
	return max
}

// formatArgForSQL renders a single bind arg as a SQL literal (used by SubstituteParams).
func formatArgForSQL(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.FormatInt(int64(x), 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case []byte:
		return "'" + escapeSQLString(string(x)) + "'"
	case string:
		return "'" + escapeSQLString(x) + "'"
	default:
		return "'" + escapeSQLString(fmt.Sprint(v)) + "'"
	}
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// SubstituteParams parses the query, replaces $1, $2, ... with formatted args, and prepends connLabel for GUI.
// On parse error or when AST has no ParamRefs, falls back to string-based replacement so substitution still works.
func SubstituteParams(sql string, args []any, connLabel string) string {
	if connLabel != "" {
		connLabel = strings.TrimSpace(connLabel)
		if connLabel != "" {
			connLabel = "[" + connLabel + "] "
		}
	}
	if len(args) == 0 {
		return connLabel + sql
	}
	tree, err := pg_query.Parse(sql)
	if err != nil || tree == nil || len(tree.Stmts) == 0 {
		return connLabel + substituteParamsFallback(sql, args)
	}
	stmt := tree.Stmts[0].Stmt
	if stmt == nil {
		return connLabel + substituteParamsFallback(sql, args)
	}
	var refs []paramRefPos
	collectParamRefs(stmt, &refs)
	if len(refs) == 0 {
		return connLabel + substituteParamsFallback(sql, args)
	}
	// PG may set location to 0 for ParamRef; we need 1-based offsets to find $n in sql.
	useFallback := false
	for _, r := range refs {
		if r.location < 1 {
			useFallback = true
			break
		}
	}
	if useFallback {
		return connLabel + substituteParamsFallback(sql, args)
	}
	// Sort by location (ascending).
	sort.Slice(refs, func(i, j int) bool { return refs[i].location < refs[j].location })
	b := []byte(sql)
	var out strings.Builder
	out.Grow(len(sql) + 64)
	prev := 0
	for _, r := range refs {
		pos := r.location - 1
		if pos < 0 {
			pos = 0
		}
		if pos >= len(b) || b[pos] != '$' {
			continue
		}
		end := pos + 1
		for end < len(b) && end >= 0 && b[end] >= '0' && b[end] <= '9' {
			end++
		}
		out.Write(b[prev:pos])
		idx := int(r.number) - 1
		if idx >= 0 && idx < len(args) {
			out.WriteString(formatArgForSQL(args[idx]))
		} else {
			out.Write(b[pos:end])
		}
		prev = end
	}
	out.Write(b[prev:])
	result := out.String()
	// If AST locations were wrong and $n remained, fall back to string replace.
	if strings.Contains(result, "$") && len(args) > 0 {
		for i := 1; i <= len(args); i++ {
			if strings.Contains(result, "$"+strconv.Itoa(i)) {
				return connLabel + substituteParamsFallback(sql, args)
			}
		}
	}
	return connLabel + result
}

// substituteParamsFallback replaces $1, $2, ... by string so substitution works when AST walk finds no ParamRefs.
func substituteParamsFallback(sql string, args []any) string {
	for i := len(args) - 1; i >= 0; i-- {
		literal := formatArgForSQL(args[i])
		sql = strings.ReplaceAll(sql, "$"+strconv.Itoa(i+1), literal)
	}
	return sql
}

// IsTransactionBegin returns true for BEGIN / START TRANSACTION.
func IsTransactionBegin(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	if t == nil {
		return false
	}
	k := t.GetKind()
	return k == pg_query.TransactionStmtKind_TRANS_STMT_BEGIN || k == pg_query.TransactionStmtKind_TRANS_STMT_START
}

// IsTransactionCommit returns true for COMMIT.
func IsTransactionCommit(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	return t != nil && t.GetKind() == pg_query.TransactionStmtKind_TRANS_STMT_COMMIT
}

// IsTransactionRollback returns true for a plain ROLLBACK (not ROLLBACK TO SAVEPOINT).
func IsTransactionRollback(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	return t != nil && t.GetKind() == pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK
}

// IsSavepoint returns true for SAVEPOINT name.
func IsSavepoint(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	return t != nil && t.GetKind() == pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT
}

// IsReleaseSavepoint returns true for RELEASE SAVEPOINT name.
func IsReleaseSavepoint(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	return t != nil && t.GetKind() == pg_query.TransactionStmtKind_TRANS_STMT_RELEASE
}

// IsRollbackToSavepoint returns true for ROLLBACK TO SAVEPOINT name.
func IsRollbackToSavepoint(stmt *pg_query.Node) bool {
	if stmt == nil {
		return false
	}
	t := stmt.GetTransactionStmt()
	return t != nil && t.GetKind() == pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO
}

// GetSavepointName returns the savepoint name for SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT.
func GetSavepointName(stmt *pg_query.Node) string {
	if stmt == nil {
		return ""
	}
	t := stmt.GetTransactionStmt()
	if t == nil {
		return ""
	}
	return t.GetSavepointName()
}

// StmtCommandTag returns the CommandComplete tag for the statement (e.g. "SELECT", "INSERT 0 1", "UPDATE 0") (AST-based).
func StmtCommandTag(stmt *pg_query.Node) string {
	kind := ClassifyStatement(stmt)
	switch kind {
	case "SELECT":
		return "SELECT"
	case "INSERT":
		return "INSERT 0 1"
	case "UPDATE":
		return "UPDATE 0"
	case "DELETE":
		return "DELETE 0"
	case "BEGIN":
		return "BEGIN"
	case "COMMIT":
		return "COMMIT"
	case "ROLLBACK":
		return "ROLLBACK"
	case "SAVEPOINT":
		return "SAVEPOINT"
	case "RELEASE":
		return "RELEASE"
	case "DEALLOCATE":
		return "DEALLOCATE"
	case "SET":
		return "SET"
	case "CREATE":
		return "CREATE"
	case "DROP":
		return "DROP"
	default:
		return "OK"
	}
}

// IsDeallocateNoise returns true when the statement is DEALLOCATE (internal driver noise for query history).
func IsDeallocateNoise(stmt *pg_query.Node) bool {
	return stmt != nil && stmt.GetDeallocateStmt() != nil
}

// --- Fallbacks when ParseStatements fails (use string-based logic) ---

// SplitCommandsFallback splits query into statements by semicolon, respecting single and double quotes.
// Use when ParseStatements fails so multi-statement execution still works (e.g. SET client_encoding='utf-8'; SELECT 1).
func SplitCommandsFallback(query string) []string {
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

// CommandTypeFromQueryFallback returns statement type from query string (e.g. "SELECT", "SAVEPOINT") when parse fails.
func CommandTypeFromQueryFallback(query string) string {
	cmdUpper := strings.ToUpper(strings.TrimSpace(query))
	for _, prefix := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "SET", "SAVEPOINT", "RELEASE", "ROLLBACK"} {
		if strings.HasPrefix(cmdUpper, prefix) {
			return prefix
		}
	}
	return "OTHER"
}

// GetCommandTagFallback returns CommandComplete tag for query when parse fails (e.g. "INSERT 0 1", "SELECT").
func GetCommandTagFallback(query string) string {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(queryUpper, "INSERT") {
		return "INSERT 0 1"
	}
	if strings.HasPrefix(queryUpper, "UPDATE") {
		return "UPDATE 0"
	}
	if strings.HasPrefix(queryUpper, "DELETE") {
		return "DELETE 0"
	}
	t := CommandTypeFromQueryFallback(query)
	if t != "OTHER" {
		return t
	}
	return "OK"
}

// ReturnsResultSetFallback returns true if query looks like SELECT or INSERT/UPDATE/DELETE with RETURNING (when parse fails).
func ReturnsResultSetFallback(query string) bool {
	cmdUpper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(cmdUpper, "SELECT") {
		return true
	}
	if strings.HasPrefix(cmdUpper, "INSERT") || strings.HasPrefix(cmdUpper, "UPDATE") || strings.HasPrefix(cmdUpper, "DELETE") {
		return strings.Contains(cmdUpper, "RETURNING")
	}
	return false
}

// ReturningColumnsFallback parses RETURNING clause from query string when parse fails. Returns nil for RETURNING * or on parse error.
func ReturningColumnsFallback(query string) []ReturningColumn {
	idx := strings.Index(strings.ToUpper(query), "RETURNING")
	if idx < 0 {
		return nil
	}
	afterReturning := strings.TrimSpace(query[idx+len("RETURNING"):])
	if afterReturning == "" {
		return nil
	}
	afterReturning = trimToEndOfStatementFallback(afterReturning)
	if afterReturning == "" {
		return nil
	}
	if strings.TrimSpace(afterReturning) == "*" {
		return nil
	}
	var cols []ReturningColumn
	for len(afterReturning) > 0 {
		afterReturning = strings.TrimSpace(afterReturning)
		if afterReturning == "" {
			break
		}
		var name string
		if strings.HasPrefix(afterReturning, `"`) {
			end := findUnescapedQuoteFallback(afterReturning[1:], '"')
			if end < 0 {
				break
			}
			name = afterReturning[1 : 1+end]
			afterReturning = strings.TrimSpace(afterReturning[1+end+1:])
		} else {
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

func trimToEndOfStatementFallback(s string) string {
	inSingle, inDouble := false, false
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

func findUnescapedQuoteFallback(s string, quote byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i
		}
	}
	return -1
}
