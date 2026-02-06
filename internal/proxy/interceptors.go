package proxy

import (
	"fmt"
	"strings"
	"time"
)

const (
	DEFAULT_SELECT_ONE    = "-- ping"
	DEFAULT_SELECT_ZERO   = "-- ping"
	FULLROLLBACK_SENTINEL = "-- fullrollback" // "pgtest rollback" response: single CommandComplete+ReadyForQuery, no DB
)

// InterceptQuery intercepta e modifica queries específicas antes da execução
func (p *PGTest) InterceptQuery(testID string, query string) (string, error) {
	queryTrimmed := strings.TrimSpace(query)
	queryUpper := strings.ToUpper(queryTrimmed)

	if strings.HasPrefix(queryUpper, "PGTEST") {
		return p.handlePGTestCommand(testID, queryTrimmed)
	}

	if strings.HasPrefix(queryUpper, "BEGIN") {
		return p.handleBegin(testID)
	}

	if strings.HasPrefix(queryUpper, "COMMIT") {
		return p.handleCommit(testID)
	}

	if strings.HasPrefix(queryUpper, "ROLLBACK") && !strings.Contains(queryUpper, "SAVEPOINT") {
		return p.handleRollback(testID)
	}

	return query, nil
}

// handlePGTestCommand processa comandos PGTEST especiais
// Usa o testID da sessão quando disponível, evitando a necessidade de passá-lo como parâmetro
func (p *PGTest) handlePGTestCommand(testID string, query string) (string, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return "", fmt.Errorf("comando pgtest inválido: %s", query)
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
		return p.RollbackBaseTransaction(testID)

	case "status":
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

// handleBegin converte BEGIN em SAVEPOINT
//
// Comportamento:
// - Se não houver transação base, cria uma primeiro (garantia de segurança)
// - Cada BEGIN cria um novo savepoint, permitindo rollback aninhado
// - O primeiro BEGIN (SavepointLevel = 0) marca o "ponto de início" desta conexão/cliente
// - Savepoints subsequentes permitem rollback parcial dentro da mesma conexão
//
// Caso de uso PHP:
// - PHP conecta → executa BEGIN → cria savepoint pgtest_v_1 (ponto de início)
// - PHP faz comandos → executa BEGIN novamente → cria savepoint pgtest_v_2
// - PHP executa ROLLBACK → faz rollback até pgtest_v_2 (não afeta pgtest_v_1)
// - PHP desconecta → próxima conexão PHP com mesmo testID pode continuar de onde parou
func (p *PGTest) handleBegin(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Session not found '%s'", testID)
	}
	return session.handleBegin(testID)
}

// handleCommit converte COMMIT em RELEASE SAVEPOINT
func (p *PGTest) handleCommit(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Transaction was not found to do a Commit on '%s'", testID)
	}
	return session.handleCommit(testID)
}

// handleRollback converte ROLLBACK em ROLLBACK TO SAVEPOINT
//
// Comportamento:
// - Se SavepointLevel > 0: faz rollback até o último savepoint e o remove
// - Se SavepointLevel = 0: não há savepoints para reverter, apenas retorna sucesso
//
// Caso de uso PHP:
// - PHP executa ROLLBACK → reverte até o último savepoint criado por esta conexão
// - Isso permite que cada conexão/cliente tenha seu próprio rollback isolado
// - O rollback não afeta outras conexões que compartilham a mesma sessão/testID
func (p *PGTest) handleRollback(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Error no session for ID: '%s'", testID)
	}
	return session.handleRollback(testID)
}

// buildStatusResultSet constrói uma query SELECT para status de uma sessão
func (p *PGTest) buildStatusResultSet(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("Session with testID '%s', was not found", testID)
	}
	return session.buildStatusResultSet(testID)
}

// buildListResultSet constrói uma query SELECT para listar todas as sessões
func (p *PGTest) buildListResultSet() (string, error) {
	sessions := p.GetAllSessions()
	if len(sessions) == 0 {
		return "SELECT NULL AS test_id, false AS active, 0 AS level, NULL AS created_at WHERE 1=0", nil
	}

	var values []string
	for testID, session := range sessions {
		session.mu.RLock()
		if session.DB == nil {
			return "", fmt.Errorf("Invalid session testID '%s'", testID)
		}
		active := session.DB.HasActiveTransaction()
		level := session.DB.SavepointLevel
		createdAt := session.CreatedAt.Format(time.RFC3339)
		session.mu.RUnlock()

		values = append(values, fmt.Sprintf(
			"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
			testID, active, level, createdAt,
		))
	}

	return strings.Join(values, " UNION ALL "), nil
}
