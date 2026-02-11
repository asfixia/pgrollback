package proxy

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const maxQueryHistory = 100

// QueryHistoryEntry is one item in the session's query history (for GUI).
type QueryHistoryEntry struct {
	Query string
	At    time.Time
}

type queryHistoryEntry struct {
	Query string
	At    time.Time
}

// isInternalNoiseQuery returns true for standard driver/internal queries we don't want in the GUI history.
// - DEALLOCATE [name]: sent by many drivers after each prepared statement use (expected protocol cleanup).
// - RELEASE SAVEPOINT pgtest_v_*: our internal savepoint release (TCL), not application logic.
func isInternalNoiseQuery(query string) bool {
	q := strings.ToUpper(strings.TrimSpace(query))
	if q == "" {
		return true
	}
	uq := strings.ToUpper(q)
	if strings.HasPrefix(uq, "DEALLOCATE") {
		if len(uq) == 10 || uq[10] == ' ' || uq[10] == '\t' {
			return true
		}
	}
	return false
}

// SetLastQuery sets the last executed query and appends it to the session's query history (max maxQueryHistory).
// Internal noise queries (e.g. DEALLOCATE from the driver) are not recorded.
// Execution order is preserved: append is the only mutation, so queryHistory is always oldest-first (index 0 = first executed).
func (d *realSessionDB) SetLastQuery(query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if isInternalNoiseQuery(query) {
		return
	}
	d.lastQuery = query
	d.queryHistory = append(d.queryHistory, queryHistoryEntry{Query: query, At: time.Now()})
	if len(d.queryHistory) > maxQueryHistory {
		d.queryHistory = d.queryHistory[1:]
	}
}

// SetLastQueryWithParams stores the query with $1, $2, ... substituted by the given args (for extended protocol).
// args are typically from bindParamsToArgs(params, formatCodes). If args is nil or empty, falls back to SetLastQuery(query).
func (d *realSessionDB) SetLastQueryWithParams(query string, args []any) {
	if len(args) == 0 {
		d.SetLastQuery(query)
		return
	}
	resolved := substituteParams(query, args)
	d.SetLastQuery(resolved)
}

func substituteParams(query string, args []any) string {
	for i := len(args) - 1; i >= 0; i-- {
		literal := formatArgForSQL(args[i])
		query = strings.ReplaceAll(query, "$"+strconv.Itoa(i+1), literal)
	}
	return query
}

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

// GetQueryHistory returns a copy of the last executed queries with timestamps (oldest first), at most maxQueryHistory.
func (d *realSessionDB) GetQueryHistory() []QueryHistoryEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.queryHistory) == 0 {
		return nil
	}
	out := make([]QueryHistoryEntry, len(d.queryHistory))
	for i, e := range d.queryHistory {
		out[i] = QueryHistoryEntry{Query: e.Query, At: e.At}
	}
	return out
}

// ClearLastQuery clears the lastQuery field (used e.g. on full rollback).
func (d *realSessionDB) ClearLastQuery() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastQuery = ""
}

// ClearQueryHistory clears the query history and last query (called when session is closed or via GUI).
func (d *realSessionDB) ClearQueryHistory() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastQuery = ""
	d.queryHistory = nil
}
