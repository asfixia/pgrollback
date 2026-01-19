package proxy

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type TestSession struct {
	TestID         string
	Conn           *pgx.Conn
	Transaction    pgx.Tx
	SavepointLevel int
	Savepoints     []string
	CreatedAt      time.Time
	LastActivity   time.Time
	mu             sync.RWMutex
}

type PGTest struct {
	Sessions     map[string]*TestSession
	PostgresHost string
	PostgresPort int
	PostgresDB   string
	PostgresUser string
	PostgresPass string
	Timeout      time.Duration
	mu           sync.RWMutex
}

func NewPGTest(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration) *PGTest {
	return &PGTest{
		Sessions:     make(map[string]*TestSession),
		PostgresHost: postgresHost,
		PostgresPort: postgresPort,
		PostgresDB:   postgresDB,
		PostgresUser: postgresUser,
		PostgresPass: postgresPass,
		Timeout:      timeout,
	}
}

func (p *PGTest) GetOrCreateSession(testID string) (*TestSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if session, exists := p.Sessions[testID]; exists {
		session.mu.Lock()
		session.LastActivity = time.Now()
		session.mu.Unlock()
		return session, nil
	}

	session, err := p.createNewSession(testID)
	if err != nil {
		return nil, err
	}

	p.Sessions[testID] = session
	return session, nil
}

func (p *PGTest) GetSession(testID string) *TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Sessions[testID]
}

func (p *PGTest) GetAllSessions() map[string]*TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	sessions := make(map[string]*TestSession)
	for k, v := range p.Sessions {
		sessions[k] = v
	}
	return sessions
}

func (p *PGTest) createNewSession(testID string) (*TestSession, error) {
	conn, err := p.connectToPostgres()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	session := &TestSession{
		TestID:         testID,
		Conn:           conn,
		Transaction:    tx,
		SavepointLevel: 0,
		Savepoints:     []string{},
		CreatedAt:      time.Now(),
		LastActivity:   time.Now(),
	}

	return session, nil
}

func (p *PGTest) connectToPostgres() (*pgx.Conn, error) {
	dsn := fmt.Sprintf("host=%s port=%d database=%s user=%s password=%s application_name=pgtest-proxy",
		p.PostgresHost, p.PostgresPort, p.PostgresDB, p.PostgresUser, p.PostgresPass)
	
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (p *PGTest) RollbackSession(testID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	session, exists := p.Sessions[testID]
	if !exists {
		return fmt.Errorf("session not found for test_id: %s", testID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.Transaction != nil {
		if err := session.Transaction.Rollback(context.Background()); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
	}

	if session.Conn != nil {
		session.Conn.Close(context.Background())
	}

	delete(p.Sessions, testID)
	return nil
}

func (p *PGTest) CleanupExpiredSessions() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for testID, session := range p.Sessions {
		session.mu.RLock()
		expired := now.Sub(session.LastActivity) > p.Timeout
		session.mu.RUnlock()

		if expired {
			session.mu.Lock()
			if session.Transaction != nil {
				session.Transaction.Rollback(context.Background())
			}
			if session.Conn != nil {
				session.Conn.Close(context.Background())
			}
			session.mu.Unlock()
			delete(p.Sessions, testID)
			cleaned++
		}
	}

	return cleaned
}

func (p *PGTest) getAdvisoryLockKey(testID string) int64 {
	hash := fnv.New64a()
	hash.Write([]byte("pgtest_" + testID))
	return int64(hash.Sum64())
}

func (p *PGTest) acquireAdvisoryLock(session *TestSession) error {
	lockKey := p.getAdvisoryLockKey(session.TestID)
	_, err := session.Conn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", lockKey)
	return err
}

func (p *PGTest) releaseAdvisoryLock(session *TestSession) error {
	lockKey := p.getAdvisoryLockKey(session.TestID)
	_, err := session.Conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockKey)
	return err
}

func (p *PGTest) ExecuteWithLock(session *TestSession, query string) (pgx.CommandTag, error) {
	if err := p.acquireAdvisoryLock(session); err != nil {
		return pgx.CommandTag{}, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	defer p.releaseAdvisoryLock(session)

	session.mu.Lock()
	session.LastActivity = time.Now()
	session.mu.Unlock()

	if session.Transaction != nil {
		return session.Transaction.Exec(context.Background(), query)
	}
	return session.Conn.Exec(context.Background(), query)
}
