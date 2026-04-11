package tstproxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"pgrollback/internal/proxy"
)

func historyHasQueryContaining(h []proxy.QueryHistoryEntry, needle string) bool {
	for _, e := range h {
		if strings.Contains(e.Query, needle) {
			return true
		}
	}
	return false
}

// TestProxyGUIQueryHistoryThroughProxy runs SQL via the proxy, then checks the same in-memory
// structures the GUI adapter uses (not HTTP): history grows, clear empties it, new queries appear after clear.
func TestProxyGUIQueryHistoryThroughProxy(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	const testID = "gui_hist_proxy"
	db, ctx, server, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()
	if db == nil || server == nil {
		return
	}

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	sess := server.PgRollback.GetSession(testID)
	if sess == nil {
		t.Fatal("expected session after connect")
	}

	markerA := "hist_gui_marker_a_7f3c"
	markerB := "hist_gui_marker_b_7f3c"
	markerC := "hist_gui_marker_c_7f3c"

	if _, err := db.ExecContext(qctx, "SELECT '"+markerA+"' AS x"); err != nil {
		t.Fatalf("first query: %v", err)
	}
	if _, err := db.ExecContext(qctx, "SELECT '"+markerB+"' AS y"); err != nil {
		t.Fatalf("second query: %v", err)
	}

	hist := sess.GUIQueryHistory()
	if len(hist) < 2 {
		t.Fatalf("expected at least 2 history entries after two SELECTs, got %d: %#v", len(hist), hist)
	}
	if !historyHasQueryContaining(hist, markerA) || !historyHasQueryContaining(hist, markerB) {
		t.Fatalf("history should contain both markers; got %d entries: %#v", len(hist), hist)
	}
	if sess.GUILastQuery() == "" || !strings.Contains(sess.GUILastQuery(), markerB) {
		t.Fatalf("GUILastQuery should end with second query; got %q", sess.GUILastQuery())
	}

	sess.ClearGUIQueryHistory()
	hist = sess.GUIQueryHistory()
	if len(hist) != 0 {
		t.Fatalf("after ClearGUIQueryHistory, want empty history, got %d entries: %#v", len(hist), hist)
	}
	if sess.GUILastQuery() != "" {
		t.Fatalf("after clear, GUILastQuery want empty, got %q", sess.GUILastQuery())
	}
	if historyHasQueryContaining(hist, markerA) || historyHasQueryContaining(hist, markerB) {
		t.Fatal("after clear, old markers should not appear in history")
	}

	if _, err := db.ExecContext(qctx, "SELECT '"+markerC+"' AS z"); err != nil {
		t.Fatalf("query after clear: %v", err)
	}
	hist = sess.GUIQueryHistory()
	if len(hist) < 1 {
		t.Fatalf("expected at least one entry after post-clear query, got %d", len(hist))
	}
	if !historyHasQueryContaining(hist, markerC) {
		t.Fatalf("post-clear history should contain new marker; got %#v", hist)
	}
	if historyHasQueryContaining(hist, markerA) || historyHasQueryContaining(hist, markerB) {
		t.Fatalf("post-clear history should not contain old markers; got %#v", hist)
	}
	if !strings.Contains(sess.GUILastQuery(), markerC) {
		t.Fatalf("GUILastQuery should be the new query; got %q", sess.GUILastQuery())
	}
}
