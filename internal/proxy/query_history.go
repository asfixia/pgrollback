package proxy

import (
	"strings"
	"time"

	sqlpkg "pgrollback/pkg/sql"
)

const maxQueryHistory = 100

// QueryHistoryEntry is one item in the session's query history (for GUI and internal storage).
type QueryHistoryEntry struct {
	Query    string
	At       time.Time
	Duration string // execution time e.g. "12.345ms"; set when query completes
}

// isInternalNoiseQuery returns true for standard driver/internal queries we don't want in the GUI history.
// - DEALLOCATE [name]: sent by many drivers after each prepared statement use (expected protocol cleanup).
func isInternalNoiseQuery(query string) bool {
	q := strings.TrimSpace(query)
	if q == "" {
		return true
	}
	stmts, err := sqlpkg.ParseStatements(q)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		uq := strings.ToUpper(q)
		return strings.HasPrefix(uq, "DEALLOCATE") && (len(uq) == 10 || (len(uq) > 10 && (uq[10] == ' ' || uq[10] == '\t')))
	}
	return sqlpkg.IsDeallocateNoise(stmts[0].Stmt)
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
	d.queryHistory = append(d.queryHistory, QueryHistoryEntry{Query: query, At: time.Now(), Duration: ""})
	if len(d.queryHistory) > maxQueryHistory {
		d.queryHistory = d.queryHistory[1:]
	}
}

// SetLastQueryWithParams stores the query with $1, $2, ... substituted by the given args (for extended protocol).
// connLabel is optional (e.g. connection remote address) and is prepended in the stored query for GUI.
// args are typically from bindParamsToArgs(params, formatCodes). If args is nil or empty, falls back to SetLastQuery(query).
func (d *realSessionDB) SetLastQueryWithParams(query string, args []any, connLabel string) {
	if len(args) == 0 {
		d.SetLastQuery(query)
		return
	}
	resolved := sqlpkg.SubstituteParams(query, args, connLabel)
	d.SetLastQuery(resolved)
}

// GetQueryHistory returns a copy of the last executed queries with timestamps (oldest first), at most maxQueryHistory.
func (d *realSessionDB) GetQueryHistory() []QueryHistoryEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.queryHistory) == 0 {
		return nil
	}
	out := make([]QueryHistoryEntry, len(d.queryHistory))
	copy(out, d.queryHistory)
	return out
}

// GetLastQueryDuration returns the duration of the last query in history (for GUI "last query" column), or "" if none.
func (d *realSessionDB) GetLastQueryDuration() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.queryHistory) == 0 {
		return ""
	}
	return d.queryHistory[len(d.queryHistory)-1].Duration
}

// UpdateLastQueryHistoryDuration sets the duration of the most recently appended query (call after execution completes).
func (d *realSessionDB) UpdateLastQueryHistoryDuration(elapsed time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updateLastQueryHistoryDurationLocked(elapsed)
}

// updateLastQueryHistoryDurationLocked updates the last history entry's duration. Caller must hold d.mu.
func (d *realSessionDB) updateLastQueryHistoryDurationLocked(elapsed time.Duration) {
	if len(d.queryHistory) == 0 {
		return
	}
	if elapsed == 0 {
		d.queryHistory[len(d.queryHistory)-1].Duration = ""
		return
	}
	d.queryHistory[len(d.queryHistory)-1].Duration = elapsed.String()
}

// ClearLastQuery removes the last query from history so GetLastQuery() returns "" or the previous query (used e.g. on full rollback).
func (d *realSessionDB) ClearLastQuery() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.queryHistory) > 0 {
		d.queryHistory = d.queryHistory[:len(d.queryHistory)-1]
	}
}

// ClearQueryHistory clears the query history (called when session is closed or via GUI).
func (d *realSessionDB) ClearQueryHistory() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queryHistory = nil
}
