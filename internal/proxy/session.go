package proxy

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type TestSession struct {
	DB             *realSessionDB // abstraction over connection + transaction; use DB.Query/Exec for all commands
	SavepointLevel int
	Savepoints     []string
	CreatedAt      time.Time
	LastActivity   time.Time
	mu             sync.RWMutex
}

type PGTest struct {
	SessionsByTestID  map[string]*TestSession
	PostgresHost      string
	PostgresPort      int
	PostgresDB        string
	PostgresUser      string
	PostgresPass      string
	Timeout           time.Duration
	SessionTimeout    time.Duration
	KeepaliveInterval time.Duration // intervalo de ping pgtest->PostgreSQL por conexão; 0 = desligado
	mu                sync.RWMutex
}

func (p *PGTest) GetTestID(session *TestSession) string {
	for testID, s := range p.SessionsByTestID {
		if s == session {
			return testID
		}
	}
	return ""
}

func NewPGTest(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration) *PGTest {
	return &PGTest{
		SessionsByTestID:  make(map[string]*TestSession),
		PostgresHost:      postgresHost,
		PostgresPort:      postgresPort,
		PostgresDB:        postgresDB,
		PostgresUser:      postgresUser,
		PostgresPass:      postgresPass,
		Timeout:           timeout,
		SessionTimeout:    sessionTimeout,
		KeepaliveInterval: keepaliveInterval,
	}
}

// GetOrCreateSession obtém uma sessão existente ou cria uma nova para o testID
//
// Comportamento de Reutilização:
// - Se já existe sessão para este testID: retorna a sessão existente
//   - A conexão PostgreSQL da sessão é reutilizada (SessionsByTestID[testID].DB)
//   - A transação PostgreSQL continua ativa (não foi commitada nem rollback)
//
// - Se não existe: cria nova sessão
//   - Cria nova conexão PostgreSQL e guarda na sessão
//   - Inicia nova transação na conexão
//
// IMPORTANTE: O mesmo testID sempre usa a mesma conexão porque há apenas uma sessão por testID,
// e a sessão guarda sua DB (connection + transaction). Tudo fica sob TestSession, indexado por testID.
func (p *PGTest) GetOrCreateSession(testID string) (*TestSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Reutiliza sessão existente se disponível
	// Isso significa que estamos reutilizando a conexão PostgreSQL e a transação
	if session, exists := p.SessionsByTestID[testID]; exists {
		session.mu.Lock()
		session.LastActivity = time.Now()
		// Verifica se a conexão ainda está válida
		if session.DB == nil || session.DB.PgConn() == nil {
			session.mu.Unlock()
			// Remove sessão inválida e cria nova
			delete(p.SessionsByTestID, testID)
		} else {
			session.mu.Unlock()
			return session, nil
		}
	}

	// Cria nova sessão para este testID (conexão fica na sessão)
	session, err := p.createNewSession(testID)
	if err != nil {
		return nil, err
	}

	p.SessionsByTestID[testID] = session
	return session, nil
}

func (p *PGTest) GetSession(testID string) *TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.SessionsByTestID[testID]
}

func (p *PGTest) GetAllSessions() map[string]*TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sessions := make(map[string]*TestSession)
	for k, v := range p.SessionsByTestID {
		sessions[k] = v
	}
	return sessions
}

// createNewSession cria uma nova sessão para o testID.
// Só é chamada quando não existe sessão para este testID; a conexão fica na sessão.
func (p *PGTest) createNewSession(testID string) (*TestSession, error) {
	if testID == "" {
		return nil, fmt.Errorf("testID is required to create a new session")
	}

	conn, err := newConnectionForTestID(p.PostgresHost, p.PostgresPort, p.PostgresDB, p.PostgresUser, p.PostgresPass, p.SessionTimeout, testID)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection for testID %s: %w", testID, err)
	}

	// Inicia nova transação na conexão
	// IMPORTANTE: Mesmo se reutilizamos a conexão, sempre criamos nova transação
	// A transação anterior (se existia) deve ter sido commitada ou rollback
	tx, err := conn.Begin(context.Background())
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	db := newSessionDB(conn, tx)
	if p.KeepaliveInterval > 0 {
		db.startKeepalive(p.KeepaliveInterval)
	}

	session := &TestSession{
		DB:             db,
		SavepointLevel: 0,
		Savepoints:     []string{},
		CreatedAt:      time.Now(),
		LastActivity:   time.Now(),
	}

	return session, nil
}

// isConnClosedOrFatal indica se o erro significa que a conexão com o PostgreSQL já está morta.
// Nesse caso, DestroySession pode tratar como sucesso ao remover a sessão do mapa.
func isConnClosedOrFatal(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Message == "conn closed" {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "conn closed") || strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") || strings.Contains(s, "connection refused") ||
		strings.Contains(s, "unexpected eof")
}

// DestroySession destrói completamente uma sessão: faz rollback da transação,
// fecha a conexão da sessão e remove do mapa.
// Se a conexão com o PostgreSQL já estiver morta (ex.: timeout), remove a sessão
// do estado e retorna nil para o cliente ter sucesso (a tarefa "encerrar sessão" foi cumprida).
func (p *PGTest) DestroySession(testID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	session, exists := p.SessionsByTestID[testID]
	if !exists {
		return fmt.Errorf("session not found for test_id: %s", testID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.DB != nil {
		if err := session.DB.rollbackTx(context.Background()); err != nil && !isConnClosedOrFatal(err) {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		_ = session.DB.close(context.Background())
		session.DB = nil
	}

	// Reseta savepoints (todos foram revertidos com o ROLLBACK)
	session.SavepointLevel = 0
	session.Savepoints = []string{}

	delete(p.SessionsByTestID, testID)
	return nil
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgtest rollback").
func (p *PGTest) RollbackBaseTransaction(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("session not found for test_id: '%s'", testID)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.DB == nil {
		return "", fmt.Errorf("session DB is nil")
	}
	return "SELECT 1", session.DB.startNewTx(context.Background())
}

// RollbackSession é um alias para DestroySession mantido para compatibilidade.
// Deprecated: Use DestroySession em vez disso.
func (p *PGTest) RollbackSession(testID string) error {
	return p.DestroySession(testID)
}

func (p *PGTest) CleanupExpiredSessions() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for testID, session := range p.SessionsByTestID {
		session.mu.RLock()
		expired := now.Sub(session.LastActivity) > p.Timeout
		session.mu.RUnlock()

		if expired {
			session.mu.Lock()
			if session.DB != nil {
				_ = session.DB.rollbackTx(context.Background())
				_ = session.DB.close(context.Background())
				session.DB = nil
			}
			session.mu.Unlock()
			delete(p.SessionsByTestID, testID)
			cleaned++
		}
	}

	return cleaned
}

func (p *PGTest) getAdvisoryLockKey(session *TestSession) int64 {
	hash := fnv.New64a()
	hash.Write([]byte("pgtest_" + p.GetTestID(session)))
	return int64(hash.Sum64())
}

func (p *PGTest) acquireAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.acquireAdvisoryLock(context.Background(), lockKey)
}

func (p *PGTest) releaseAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.releaseAdvisoryLock(context.Background(), lockKey)
}

func (p *PGTest) ExecuteWithLock(session *TestSession, query string) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	if err := p.acquireAdvisoryLock(session); err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	defer p.releaseAdvisoryLock(session)

	session.mu.Lock()
	session.LastActivity = time.Now()
	session.mu.Unlock()

	_, err := session.DB.Exec(context.Background(), query)
	return err
}

// GetSavepointLevel retorna o nível atual de savepoint da sessão
func (s *TestSession) GetSavepointLevel() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.SavepointLevel
}

// GetSavepoints retorna a lista de savepoints da sessão
func (s *TestSession) GetSavepoints() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Retorna uma cópia para evitar modificações externas
	result := make([]string, len(s.Savepoints))
	copy(result, s.Savepoints)
	return result
}
