package proxy

import (
	"fmt"
	"net/http"
	"time"

	"pgrollback/internal/proxy/gui"
)

// sessionProviderAdapter adapts *Server to gui.SessionProvider so the GUI package does not import proxy.
type sessionProviderAdapter struct {
	s *Server
}

func (a *sessionProviderAdapter) GetSessions() []gui.SessionInfo {
	sessions := a.s.PgRollback.GetAllSessions()
	list := make([]gui.SessionInfo, 0, len(sessions))
	for testID, session := range sessions {
		inTransaction := false
		lastQuery := ""
		var queryHistory []gui.QueryHistoryItem
		lastQueryDuration := session.GetLastQueryDuration()
		if session.DB != nil {
			inTransaction = session.DB.HasOpenUserTransaction()
			lastQuery = session.DB.GetLastQuery()
			entries := session.DB.GetQueryHistory()
			queryHistory = make([]gui.QueryHistoryItem, len(entries))
			for i, e := range entries {
				queryHistory[i] = gui.QueryHistoryItem{Query: e.Query, At: e.At.Format(time.RFC3339), Duration: e.Duration}
			}
		}
		list = append(list, gui.SessionInfo{
			TestID:            testID,
			InTransaction:     inTransaction,
			LastQuery:         lastQuery,
			LastQueryDuration: lastQueryDuration,
			QueryHistory:      queryHistory,
		})
	}
	return list
}

func (a *sessionProviderAdapter) DestroySession(testID string) error {
	return a.s.PgRollback.DestroySession(testID)
}

func (a *sessionProviderAdapter) ClearHistory(testID string) error {
	session := a.s.PgRollback.GetSession(testID)
	if session == nil {
		return fmt.Errorf("session not found")
	}
	if session.DB != nil {
		session.DB.ClearQueryHistory()
	}
	return nil
}

func (a *sessionProviderAdapter) DestroyAllSessions() (int, error) {
	sessions := a.s.PgRollback.GetAllSessions()
	n := 0
	for testID := range sessions {
		if err := a.s.PgRollback.DestroySession(testID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// guiMux returns the HTTP handler for the GUI (same-port: /, /gui, /gui/, /api/...).
func guiMux(server *Server) http.Handler {
	return gui.NewMux(&sessionProviderAdapter{s: server})
}

// StartGUIServer starts the GUI HTTP server on a separate port (backward compatibility).
// Prefer same-port GUI via NewServer(..., true) so the GUI is at http://host:port/gui.
func StartGUIServer(server *Server, host string, port int) (stop func(), err error) {
	return gui.StartGUIServer(&sessionProviderAdapter{s: server}, host, port)
}
