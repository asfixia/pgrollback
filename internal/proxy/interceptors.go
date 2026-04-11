package proxy

import (
	"fmt"
	"log"
	"strings"
	"time"

	"pgrollback/pkg/sql"
)

const (
	DEFAULT_SELECT_ONE    = "-- ping"
	DEFAULT_SELECT_ZERO   = "-- ping"
	FULLROLLBACK_SENTINEL = "-- fullrollback"          // "pgrollback rollback" response: single CommandComplete+ReadyForQuery, no DB
	DISCONNECT_SENTINEL   = "-- pgrollback disconnect" // "pgrollback disconnect" response: single CommandComplete+ReadyForQuery, no DB
)

// InterceptQuery intercepta e modifica queries específicas antes da execução.
// connID is the connection making the request; pass 0 when there is no connection (e.g. tests). When connID != 0, BEGIN fails if another connection already has an open transaction.
// PGROLLBACK commands are checked first (not valid SQL). TCL (BEGIN/COMMIT/ROLLBACK) is detected via AST when parse succeeds.
func (p *PgRollback) InterceptQuery(testID string, query string, connID ConnectionID) (string, error) {
	queryTrimmed := strings.TrimSpace(query)
	queryUpper := strings.ToUpper(queryTrimmed)

	if strings.HasPrefix(queryUpper, "PGROLLBACK") {
		return p.interceptPgRollbackCommand(testID, queryTrimmed)
	}

	// Multi-statement simple query starting with BEGIN: intercept only the first statement.
	// Otherwise ParseStatements sees BEGIN first and InterceptQuery would replace the entire
	// string with SAVEPOINT, dropping CREATE TABLE / INSERT / etc.
	parts := sql.SplitCommandsFallback(queryTrimmed)
	var nonEmpty []string
	for _, part := range parts {
		if t := strings.TrimSpace(part); t != "" {
			nonEmpty = append(nonEmpty, t)
		}
	}
	if len(nonEmpty) >= 2 {
		first := nonEmpty[0]
		firstUpper := strings.ToUpper(first)
		if strings.HasPrefix(firstUpper, "BEGIN") && !strings.Contains(firstUpper, "SAVEPOINT") {
			isBegin := false
			if stmts1, err1 := sql.ParseStatements(first); err1 == nil && len(stmts1) > 0 && stmts1[0].Stmt != nil {
				if sql.IsTransactionBegin(stmts1[0].Stmt) {
					isBegin = true
				}
			}
			if !isBegin && strings.HasPrefix(strings.TrimSpace(firstUpper), "BEGIN") {
				isBegin = true
			}
			if isBegin {
				rewrittenFirst, err := p.interceptBegin(testID, connID)
				if err != nil {
					return "", err
				}
				rest := strings.Join(nonEmpty[1:], "; ")
				if rest == "" {
					return rewrittenFirst, nil
				}
				return rewrittenFirst + "; " + rest, nil
			}
		}
	}

	stmts, err := sql.ParseStatements(query)
	if err == nil && len(stmts) > 0 && stmts[0].Stmt != nil {
		stmt := stmts[0].Stmt
		if sql.IsTransactionBegin(stmt) {
			return p.interceptBegin(testID, connID)
		}
		if sql.IsTransactionCommit(stmt) {
			return p.interceptCommit(testID)
		}
		if sql.IsTransactionRollback(stmt) {
			return p.interceptRollback(testID)
		}
		return query, nil
	}

	// Fallback when parse fails (e.g. malformed SQL)
	if strings.HasPrefix(queryUpper, "BEGIN") {
		return p.interceptBegin(testID, connID)
	}
	if strings.HasPrefix(queryUpper, "COMMIT") {
		return p.interceptCommit(testID)
	}
	if strings.HasPrefix(queryUpper, "ROLLBACK") && !strings.Contains(queryUpper, "SAVEPOINT") {
		return p.interceptRollback(testID)
	}

	return query, nil
}

// interceptPgRollbackCommand processa comandos PgRollback especiais
// Usa o testID da sessão quando disponível, evitando a necessidade de passá-lo como parâmetro
func (p *PgRollback) interceptPgRollbackCommand(testID string, query string) (string, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return "", fmt.Errorf("comando pgrollback inválido: %s", query)
	}

	action := strings.ToLower(parts[1])

	switch action {
	case "begin":
		_, err := p.GetOrCreateSession(testID)
		if err != nil {
			return "", err
		}
		return DEFAULT_SELECT_ONE, nil

	case "rollback":
		log.Printf("[PGROLLBACK] rollback requested for testID=%s", testID)
		return p.RollbackBaseTransaction(testID)

	case "disconnect":
		log.Printf("[PGROLLBACK] disconnect requested for testID=%s", testID)
		if session := p.GetSession(testID); session != nil {
			session.MarkDisconnectRequested()
		}
		return DISCONNECT_SENTINEL, nil

	case "status":
		return p.buildStatusResultSet(testID)

	case "list":
		return p.buildListResultSet()

	case "cleanup":
		cleaned, err := p.CleanupExpiredSessions()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SELECT %d AS cleaned", cleaned), nil

	default:
		return "", fmt.Errorf("ação desconhecida: %s", action)
	}
}

// interceptBegin converte BEGIN em SAVEPOINT
//
// Comportamento:
// - Se não houver transação base, cria uma primeiro (garantia de segurança)
// - Cada BEGIN cria um novo savepoint, permitindo rollback aninhado
// - O primeiro BEGIN (SavepointLevel = 0) marca o "ponto de início" desta conexão/cliente
// - Savepoints subsequentes permitem rollback parcial dentro da mesma conexão
// - When connID != 0, BEGIN fails with ErrOnlyOneTransactionAtATime if connectionWithOpenTx is another connection.
//
// Caso de uso PHP:
// - PHP conecta → executa BEGIN → cria savepoint pgrollback_v_1 (ponto de início)
// - PHP faz comandos → executa BEGIN novamente → cria savepoint pgrollback_v_2
// - PHP executa ROLLBACK → faz rollback até pgrollback_v_2 (não afeta pgrollback_v_1)
// - PHP desconecta → próxima conexão PHP com mesmo testID pode continuar de onde parou
func (p *PgRollback) interceptBegin(testID string, connID ConnectionID) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Session not found '%s'", testID)
	}
	return session.handleBegin(testID, connID)
}

// interceptCommit converte COMMIT em RELEASE SAVEPOINT
func (p *PgRollback) interceptCommit(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Transaction was not found to do a Commit on '%s'", testID)
	}
	return session.handleCommit(testID)
}

// interceptRollback converte ROLLBACK em ROLLBACK TO SAVEPOINT
//
// Comportamento:
// - Se SavepointLevel > 0: faz rollback até o último savepoint e o remove
// - Se SavepointLevel = 0: não há savepoints para reverter, apenas retorna sucesso
//
// Caso de uso PHP:
// - PHP executa ROLLBACK → reverte até o último savepoint criado por esta conexão
// - Isso permite que cada conexão/cliente tenha seu próprio rollback isolado
// - O rollback não afeta outras conexões que compartilham a mesma sessão/testID
func (p *PgRollback) interceptRollback(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Error no session for ID: '%s'", testID)
	}
	return session.handleRollback(testID)
}

// buildStatusResultSet constrói uma query SELECT para status de uma sessão
func (p *PgRollback) buildStatusResultSet(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Session with testID '%s', was not found", testID)
	}
	return session.buildStatusResultSet(testID)
}

// buildListResultSet constrói uma query SELECT para listar todas as sessões
func (p *PgRollback) buildListResultSet() (string, error) {
	sessions := p.GetAllSessions()
	if len(sessions) == 0 {
		return "SELECT NULL AS test_id, false AS active, 0 AS level, NULL AS created_at WHERE 1=0", nil
	}

	var values []string
	for testID, session := range sessions {
		session.mu.RLock()
		if session.DB == nil {
			session.mu.RUnlock()
			return "", fmt.Errorf("Invalid session testID '%s'", testID)
		}
		active := session.DB.HasActiveTransaction()
		level := session.DB.GetSavepointLevel()
		createdAt := session.CreatedAt.Format(time.RFC3339)
		session.mu.RUnlock()

		values = append(values, fmt.Sprintf(
			"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
			testID, active, level, createdAt,
		))
	}

	return strings.Join(values, " UNION ALL "), nil
}
