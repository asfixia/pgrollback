package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgproto3/v3"
)

func (s *Server) handleQuery(clientConn net.Conn, session *TestSession, query string) error {
	interceptedQuery, err := s.pgtest.InterceptQuery(session, query)
	if err != nil {
		return s.sendErrorResponse(clientConn, err.Error())
	}

	if interceptedQuery == "" {
		return s.sendCommandComplete(clientConn, "SELECT")
	}

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(interceptedQuery)), "SELECT") && 
		(strings.Contains(interceptedQuery, "test_id") || strings.Contains(interceptedQuery, "cleaned")) {
		return s.handleResultSetQuery(clientConn, session, interceptedQuery)
	}

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(interceptedQuery)), "SELECT 1") {
		return s.sendCommandComplete(clientConn, "SELECT")
	}

	result, err := s.pgtest.ExecuteWithLock(session, interceptedQuery)
	if err != nil {
		return s.sendErrorResponse(clientConn, err.Error())
	}

	return s.sendCommandCompleteWithTag(clientConn, interceptedQuery, result)
}

func (s *Server) sendErrorResponse(clientConn net.Conn, message string) error {
	backend := pgproto3.NewBackend(nil, clientConn)
	return backend.Send(&pgproto3.ErrorResponse{
		Message: message,
	})
}

func (s *Server) sendCommandComplete(clientConn net.Conn, query string) error {
	return s.sendCommandCompleteWithTag(clientConn, query, nil)
}

func (s *Server) sendCommandCompleteWithTag(clientConn net.Conn, query string, result interface{}) error {
	backend := pgproto3.NewBackend(nil, clientConn)
	tag := "SELECT"
	
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(queryUpper, "INSERT") {
		tag = "INSERT 0 1"
	} else if strings.HasPrefix(queryUpper, "UPDATE") {
		tag = "UPDATE 0"
	} else if strings.HasPrefix(queryUpper, "DELETE") {
		tag = "DELETE 0"
	} else if strings.HasPrefix(queryUpper, "SAVEPOINT") {
		tag = "SAVEPOINT"
	} else if strings.HasPrefix(queryUpper, "RELEASE") {
		tag = "RELEASE"
	} else if strings.HasPrefix(queryUpper, "ROLLBACK") {
		tag = "ROLLBACK"
	}
	
	if err := backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)}); err != nil {
		return err
	}
	
	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *Server) handleResultSetQuery(clientConn net.Conn, session *TestSession, query string) error {
	rows, err := session.Conn.Query(context.Background(), query)
	if err != nil {
		return s.sendErrorResponse(clientConn, err.Error())
	}
	defer rows.Close()

	backend := pgproto3.NewBackend(nil, clientConn)

	fieldDescriptions := rows.FieldDescriptions()
	fields := make([]*pgproto3.FieldDescription, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		fields[i] = &pgproto3.FieldDescription{
			Name:                 []byte(fd.Name),
			TableOID:             fd.TableOID,
			TableAttributeNumber: fd.TableAttributeNumber,
			DataTypeOID:          fd.DataTypeOID,
			DataTypeSize:         fd.DataTypeSize,
			TypeModifier:         fd.TypeModifier,
			Format:               fd.Format,
		}
	}

	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return err
	}

	rowCount := 0
	for rows.Next() {
		rowCount++
		values, err := rows.Values()
		if err != nil {
			return err
		}

		rowValues := make([][]byte, len(values))
		for i, v := range values {
			if v == nil {
				rowValues[i] = nil
			} else {
				rowValues[i] = []byte(fmt.Sprintf("%v", v))
			}
		}

		if err := backend.Send(&pgproto3.DataRow{Values: rowValues}); err != nil {
			return err
		}
	}

	if err := backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCount))}); err != nil {
		return err
	}

	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (p *PGTest) InterceptQuery(session *TestSession, query string) (string, error) {
	queryTrimmed := strings.TrimSpace(query)
	queryUpper := strings.ToUpper(queryTrimmed)

	if strings.HasPrefix(queryUpper, "PGTEST") {
		return p.handlePGTestCommand(queryTrimmed)
	}

	if strings.HasPrefix(queryUpper, "BEGIN") {
		return p.handleBegin(session)
	}

	if strings.HasPrefix(queryUpper, "COMMIT") {
		return p.handleCommit(session)
	}

	if strings.HasPrefix(queryUpper, "ROLLBACK") {
		return p.handleRollback(session)
	}

	return query, nil
}

func (p *PGTest) handlePGTestCommand(query string) (string, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return "", fmt.Errorf("comando pgtest inválido: %s", query)
	}

	action := strings.ToLower(parts[1])

	switch action {
	case "begin":
		if len(parts) < 3 {
			return "", fmt.Errorf("pgtest begin requer test_id")
		}
		testID := parts[2]
		_, err := p.GetOrCreateSession(testID)
		if err != nil {
			return "", err
		}
		return "SELECT 1", nil

	case "rollback":
		if len(parts) < 3 {
			return "", fmt.Errorf("pgtest rollback requer test_id")
		}
		testID := parts[2]
		err := p.RollbackSession(testID)
		if err != nil {
			return "", err
		}
		return "SELECT 1", nil

	case "status":
		if len(parts) < 3 {
			return "", fmt.Errorf("pgtest status requer test_id")
		}
		testID := parts[2]
		return p.buildStatusResultSet(testID)

	case "list":
		return p.buildListResultSet()

	case "cleanup":
		cleaned := p.CleanupExpiredSessions()
		return fmt.Sprintf("SELECT %d AS cleaned", cleaned), nil

	default:
		return "", fmt.Errorf("ação desconhecida: %s", action)
	}
}

func (p *PGTest) handleBegin(session *TestSession) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.SavepointLevel++
	savepointName := fmt.Sprintf("sp_%d", session.SavepointLevel)
	session.Savepoints = append(session.Savepoints, savepointName)

	return fmt.Sprintf("SAVEPOINT %s", savepointName), nil
}

func (p *PGTest) handleCommit(session *TestSession) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.SavepointLevel > 0 {
		savepointName := session.Savepoints[len(session.Savepoints)-1]
		session.Savepoints = session.Savepoints[:len(session.Savepoints)-1]
		session.SavepointLevel--

		return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
	}

	return "SELECT 1", nil
}

func (p *PGTest) handleRollback(session *TestSession) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.SavepointLevel > 0 {
		savepointName := session.Savepoints[len(session.Savepoints)-1]
		session.Savepoints = session.Savepoints[:len(session.Savepoints)-1]
		session.SavepointLevel--

		return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", savepointName, savepointName), nil
	}

	return "SELECT 1", nil
}

func (p *PGTest) buildStatusResultSet(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "SELECT NULL AS test_id, false AS active, 0 AS level, NULL AS created_at", nil
	}

	session.mu.RLock()
	active := session.Transaction != nil
	level := session.SavepointLevel
	createdAt := session.CreatedAt.Format(time.RFC3339)
	session.mu.RUnlock()

	return fmt.Sprintf(
		"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
		testID, active, level, createdAt,
	), nil
}

func (p *PGTest) buildListResultSet() (string, error) {
	sessions := p.GetAllSessions()
	if len(sessions) == 0 {
		return "SELECT NULL AS test_id, false AS active, 0 AS level, NULL AS created_at WHERE 1=0", nil
	}

	var values []string
	for testID, session := range sessions {
		session.mu.RLock()
		active := session.Transaction != nil
		level := session.SavepointLevel
		createdAt := session.CreatedAt.Format(time.RFC3339)
		session.mu.RUnlock()

		values = append(values, fmt.Sprintf(
			"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
			testID, active, level, createdAt,
		))
	}

	return strings.Join(values, " UNION ALL "), nil
}
