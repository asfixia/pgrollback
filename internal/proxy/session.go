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
	"github.com/jackc/pgx/v5/pgproto3"
)

// BackendStartupCache holds ParameterStatus and BackendKeyData from the real PostgreSQL
// so we can replay them to clients when they connect to pgrollback instead of hardcoded values.
type BackendStartupCache struct {
	ParameterStatuses []pgproto3.ParameterStatus // order preserved for consistent client behavior
	BackendKeyData    pgproto3.BackendKeyData
}

// Well-known parameter names that PostgreSQL sends after connection (we copy these from the real server).
var backendStartupParameterNames = []string{
	"server_version", "server_encoding", "client_encoding", "DateStyle", "TimeZone", "session_authorization",
	"integer_datetimes", "standard_conforming_strings", "application_name", "default_transaction_read_only",
	"intervalStyle", "is_superuser",
}

type TestSession struct {
	DB                  *realSessionDB // abstraction over connection + transaction; use DB.Query/Exec for all commands
	TestID              string
	CreatedAt           time.Time
	LastActivity        time.Time
	DisconnectRequested bool
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.RWMutex
}

type PgRollback struct {
	SessionsByTestID  map[string]*TestSession
	PostgresHost      string
	PostgresPort      int
	PostgresDB        string
	PostgresUser      string
	PostgresPass      string
	Timeout           time.Duration
	SessionTimeout    time.Duration
	KeepaliveInterval time.Duration // intervalo de ping pgrollback->PostgreSQL por conexão; 0 = desligado
	mu                sync.RWMutex

	// backendStartupCache is filled from the first real PostgreSQL connection and replayed to clients.
	backendStartupCache *BackendStartupCache
}

// GetLastQueryDuration returns the last query execution duration (e.g. "12.345ms") for GUI, derived from the last history entry.
func (s *TestSession) GetLastQueryDuration() string {
	if s.DB == nil {
		return ""
	}
	return s.DB.Gui.GetLastQueryDuration()
}

func (p *PgRollback) GetTestID(session *TestSession) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for testID, s := range p.SessionsByTestID {
		if s == session {
			return testID
		}
	}
	return ""
}

// MarkDisconnectRequested marks this session so that runDisconnectCleanup will fully destroy it
// (rollback transaction, close connection, remove from map) when the client disconnects.
func (s *TestSession) MarkDisconnectRequested() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DisconnectRequested = true
}

// ShouldDisconnectOnCleanup reports whether this session was marked for destruction on disconnect.
func (s *TestSession) ShouldDisconnectOnCleanup() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ShouldDisconnectOnCleanupLocked()
}

func (s *TestSession) ShouldDisconnectOnCleanupLocked() bool {
	return s.DisconnectRequested
}

// Context returns the session's context, or context.Background() if not initialized.
func (s *TestSession) Context() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// Cancel cancels the session's context (if any). Safe to call multiple times.
func (s *TestSession) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CancelLocked()
}

func (s *TestSession) CancelLocked() {
	if s.cancel != nil {
		s.cancel()
		// Leave ctx as-is; it remains canceled and useful for checks.
	}
}

func NewPgRollback(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration) *PgRollback {
	return &PgRollback{
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
func (p *PgRollback) GetOrCreateSession(testID string) (*TestSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if testID == "" {
		return nil, fmt.Errorf("testID is required")
	}
	if session := p.getUsableExistingSessionLocked(testID); session != nil {
		return session, nil
	}
	newSession, err := p.createNewSessionLocked(testID)
	if err != nil {
		return nil, err
	}
	p.SessionsByTestID[testID] = newSession
	return newSession, nil
}

// getUsableExistingSessionLocked returns an existing session if it is usable; otherwise it removes it from the map.
// Caller must hold p.mu (write lock) because it may delete from SessionsByTestID.
func (p *PgRollback) getUsableExistingSessionLocked(testID string) *TestSession {
	// Fast path: reuse existing session (brief lock).
	if session, exists := p.SessionsByTestID[testID]; exists {
		session.mu.Lock()
		session.LastActivity = time.Now()
		session.mu.Unlock()

		if session.DB != nil && session.DB.PgConnLocked() != nil && !session.DB.PgConnLocked().IsClosed() {
			return session
		}
		delete(p.SessionsByTestID, testID)
	}
	return nil
}

func (p *PgRollback) GetSession(testID string) *TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.GetSessionLocked(testID)
}

func (p *PgRollback) GetSessionLocked(testID string) *TestSession {
	return p.SessionsByTestID[testID]
}

func (p *PgRollback) GetAllSessions() map[string]*TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sessions := make(map[string]*TestSession)
	for k, v := range p.SessionsByTestID {
		sessions[k] = v
	}
	return sessions
}

// createNewSessionLocked cria uma nova sessão para o testID.
// Só é chamada quando não existe sessão para este testID; a conexão fica na sessão.
func (p *PgRollback) createNewSessionLocked(testID string) (*TestSession, error) {
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
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		cancel()
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	//#TODO (IA MANTER ISSO AQUI) Danilo Quero revisar pra usar o keep interval
	db := newSessionDB(conn, tx, ctx)
	p.fillBackendStartupCacheIfNeeded(db.PgConn())
	//if p.KeepaliveInterval > 0 {
	//	db.startKeepalive(p.KeepaliveInterval)
	//}

	session := &TestSession{
		DB:           db,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}

	return session, nil
}

// fillBackendStartupCacheIfNeeded copies ParameterStatus from the real PostgreSQL connection into the
// cache so we can replay them to clients. Called when creating a new session; only fills once.
// BackendKeyData is not exposed by pgx PgConn, so we keep a default value.
func (p *PgRollback) fillBackendStartupCacheIfNeeded(pgConn *pgconn.PgConn) {
	if pgConn == nil {
		return
	}
	if p.backendStartupCache != nil {
		return
	}
	var params []pgproto3.ParameterStatus
	for _, name := range backendStartupParameterNames {
		value := pgConn.ParameterStatus(name)
		if value != "" {
			params = append(params, pgproto3.ParameterStatus{Name: name, Value: value})
		}
	}
	p.backendStartupCache = &BackendStartupCache{
		ParameterStatuses: params,
		BackendKeyData:    pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890}, // pgx does not expose; keep default
	}
}

// GetBackendStartupCache returns the cached backend startup messages from the real PostgreSQL, or nil if not yet filled.
func (p *PgRollback) GetBackendStartupCache() *BackendStartupCache {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.backendStartupCache
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
func (p *PgRollback) DestroySession(testID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, exists := p.SessionsByTestID[testID]
	if !exists {
		return fmt.Errorf("session not found for test_id: %s", testID)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return p.DestroySessionLocked(testID, session)
}

func (p *PgRollback) DestroySessionLocked(testID string, session *TestSession) error {
	if session == nil || session.DB == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := session.DB.close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close session: '%s': %w", testID, err)
	}
	session.DB = nil
	delete(p.SessionsByTestID, testID)
	return nil
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgrollback rollback").
func (p *PgRollback) RollbackBaseTransaction(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("session not found for test_id: '%s'", testID)
	}
	return session.RollbackBaseTransaction(testID)
}

// RollbackSession é um alias para DestroySession mantido para compatibilidade.
// Deprecated: Use DestroySession em vez disso.
func (p *PgRollback) RollbackSession(testID string) error {
	return p.DestroySession(testID)
}

// sessionIdleExpired reports whether idle time since LastActivity exceeds p.Timeout.
// Caller must hold session.mu (read or write lock).
func (p *PgRollback) sessionIdleExpired(session *TestSession, timeToConsider time.Time) bool {
	return timeToConsider.Sub(session.LastActivity) > p.Timeout
}

func (p *PgRollback) CleanupExpiredSessions() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for testID, session := range p.SessionsByTestID {
		session.mu.RLock()
		expired := p.sessionIdleExpired(session, now)
		session.mu.RUnlock()

		if expired {
			session.mu.Lock()
			if session.DB != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := session.DB.close(ctx)
				cancel()
				if err != nil {
					session.mu.Unlock()
					return cleaned, fmt.Errorf("failed to close session: %w", err)
				}
				session.DB = nil
			}
			session.mu.Unlock()
			delete(p.SessionsByTestID, testID)
			cleaned++
		}
	}

	return cleaned, nil
}

func (p *PgRollback) getAdvisoryLockKey(session *TestSession) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	hash := fnv.New64a()
	hash.Write([]byte("pgrollback_" + p.GetTestID(session)))
	return int64(hash.Sum64())
}

func (p *PgRollback) acquireAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.acquireAdvisoryLock(session.Context(), lockKey)
}

func (p *PgRollback) releaseAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.releaseAdvisoryLock(session.Context(), lockKey)
}

// ExecuteWithLock runs query on the session's shared backend transaction while holding a per-test_id
// PostgreSQL advisory lock. The query is executed via SafeExec (guard savepoint) so a SQL error does
// not abort the outer transaction — the next advisory lock / proxy command still sees a healthy tx.
func (p *PgRollback) ExecuteWithLock(session *TestSession, query string) error {
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

	_, err := session.DB.SafeExec(session.Context(), query)
	return err
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
// - PHP conecta → executa BEGIN → cria savepoint pgrollback_v_1 (ponto de início)
// - PHP faz comandos → executa BEGIN novamente → cria savepoint pgrollback_v_2
// - PHP executa ROLLBACK → faz rollback até pgrollback_v_2 (não afeta pgrollback_v_1)
// - PHP desconecta → próxima conexão PHP com mesmo testID pode continuar de onde parou
func (s *TestSession) handleBegin(testID string, connID ConnectionID) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Begin TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleBegin(testID, connID)
}

// handleCommit converte COMMIT em RELEASE SAVEPOINT
func (s *TestSession) handleCommit(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Commit TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleCommit(testID)
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
func (s *TestSession) handleRollback(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Rollback TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleRollback(testID)
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgrollback rollback").
// Returns FULLROLLBACK_SENTINEL so the proxy sends exactly one CommandComplete+ReadyForQuery without
// forwarding to the DB, avoiding response attribution issues with the next query (e.g. ResetSession ping).
func (s *TestSession) RollbackBaseTransaction(testID string) (string, error) {
	return FULLROLLBACK_SENTINEL, s.DB.startNewTx(s.Context())
}

// buildStatusResultSet constrói uma query SELECT para status de uma sessão
func (s *TestSession) buildStatusResultSet(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Session with testID '%s', doesnt have a real connection.", testID)
	}
	return s.DB.buildStatusResultSet(s.CreatedAt, testID)
}
