package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// realSessionDB encapsulates the PostgreSQL connection and its active transaction.
// - The connection (conn) is only used for transaction control: Begin, Rollback base, Close, keepalive, advisory lock.
// - All data operations (Query, Exec) go through the transaction so commands are never run outside the transaction.
//
// Callers use Query/Exec; the abstraction ensures the right object (tx) is used.
// You cannot "get the transaction from Conn" in pgx—Conn.Begin() returns a Tx, so both are stored and managed here.
type realSessionDB struct {
	conn               *pgx.Conn
	tx                 pgx.Tx
	mu                 sync.RWMutex
	SavepointLevel     int
	stopKeepalive      func()
	lastQuery          string
	preparedStatements map[string]string // statement name -> intercepted query (Extended Query); always non-nil (set in newSessionDB)
	portalToStatement  map[string]string // portal name -> statement name (Extended Query); always non-nil (set in newSessionDB)
}

func (d *realSessionDB) GetSavepointLevel() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.SavepointLevel
}

func (d *realSessionDB) GetSavepointName() string {
	return fmt.Sprintf("pgtest_v_%d", d.SavepointLevel)
}

// SetPreparedStatement stores the intercepted query for the given statement name (Extended Query).
// Caller must hold no locks; the method uses d.mu.
func (d *realSessionDB) SetPreparedStatement(statementName, query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.preparedStatements[statementName] = query
}

// BindPortal records which portal is bound to which statement (Extended Query).
// Caller must hold no locks; the method uses d.mu.
func (d *realSessionDB) BindPortal(portalName, statementName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.portalToStatement[portalName] = statementName
}

// QueryForPortal returns the query for the given portal, or ("", false) if not found.
// Caller must hold no locks; the method uses d.mu.
func (d *realSessionDB) QueryForPortal(portalName string) (query string, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	statementName := d.portalToStatement[portalName]
	query = d.preparedStatements[statementName]
	return query, statementName != "" && query != ""
}

// CloseStatementOrPortal removes the statement or portal from the maps (objectType 'S' or 'P').
// Caller must hold no locks; the method uses d.mu.
func (d *realSessionDB) CloseStatementOrPortal(objectType byte, name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch objectType {
	case 'S':
		delete(d.preparedStatements, name)
	case 'P':
		delete(d.portalToStatement, name)
	}
}

// ClearLastQuery clears the lastQuery field (used e.g. on full rollback).
func (d *realSessionDB) ClearLastQuery() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastQuery = ""
}

// Ensure realSessionDB implements pgxQueryer (used by tx_guard).
var _ pgxQueryer = (*realSessionDB)(nil)

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
func (d *realSessionDB) handleRollback(testID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.SavepointLevel > 0 {
		savepointName := d.GetSavepointName()
		d.SavepointLevel--

		// Faz rollback até o savepoint e depois o remove (RELEASE)
		// Isso reverte todas as mudanças feitas após este savepoint
		return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", savepointName, savepointName), nil
	}

	// Não há savepoints para reverter
	// Retorna sucesso sem fazer nada (não há nada para reverter desta conexão)
	return DEFAULT_SELECT_ONE, nil
}

func (d *realSessionDB) buildStatusResultSet(createdAt time.Time, testID string) (string, error) {
	d.mu.RLock()
	active := d.HasActiveTransaction()
	level := d.SavepointLevel
	d.mu.RUnlock()

	return fmt.Sprintf(
		"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
		testID, active, level, createdAt.Format(time.RFC3339),
	), nil
}

// Query runs a query in the current transaction. Returns an error if there is no active transaction.
func (d *realSessionDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.mu.RLock()
	tx := d.tx
	defer d.mu.RUnlock()
	if tx == nil {
		return nil, fmt.Errorf("no active transaction: use BeginTx first")
	}
	return tx.Query(ctx, sql, args...)
}

func (d *realSessionDB) handleCommit(testID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.SavepointLevel > 0 {
		savepointName := d.GetSavepointName()
		d.SavepointLevel--
		return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
	}

	return DEFAULT_SELECT_ONE, nil
}

func (d *realSessionDB) handleBegin(testID string) (string, error) {

	if !d.HasActiveTransaction() {
		return "", fmt.Errorf("no active transaction: use BeginTx first")
	}

	// Garantia de segurança: se não houver transação base, cria uma primeiro
	// Isso pode acontecer se a transação foi commitada/rollback mas a sessão ainda existe
	// Em testes unitários (session.DB == nil ou conn nil), BeginTx é no-op
	if err := d.beginTx(context.Background()); err != nil {
		return "", fmt.Errorf("Failed to Begin a transaction: %w", err)
	}

	d.mu.Lock()
	// Cada BEGIN cria um novo savepoint, permitindo rollback aninhado
	// Usa prefixo "pgtest_v_" para garantir que não conflite com savepoints criados pelo usuário
	d.SavepointLevel++
	d.mu.Unlock()

	return fmt.Sprintf("SAVEPOINT %s", d.GetSavepointName()), nil
}

// Exec runs a command in the current transaction. Returns an error if there is no active transaction.
func (d *realSessionDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.mu.RLock()
	tx := d.tx
	defer d.mu.RUnlock()
	if tx == nil {
		var zero pgconn.CommandTag
		return zero, fmt.Errorf("no active transaction: use BeginTx first")
	}
	return tx.Exec(ctx, sql, args...)
}

// Danilo isso aqui é só pra ser usado no savepoint (o commit nele é tratado como releasepoint)
func commitSavePoint(ctx context.Context, savepoint pgx.Tx) {
	if savepoint == nil {
		return
	}
	err := savepoint.Commit(ctx)
	if err != nil {
		log.Fatalf("Failed to remove a savePoint")
	}
}

func (d *realSessionDB) SafeQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	savePoint, err := d.tx.Begin(ctx)
	if err != nil || savePoint == nil {
		return nil, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, sql: '''%s'''", err, sql)
	}
	rows, err := savePoint.Query(ctx, sql)
	errList := []error{}
	if err != nil {
		errList = append(errList, fmt.Errorf("Falha ao executar consulta"))
		if rollbackErr := savePoint.Rollback(ctx); rollbackErr != nil {
			errList = append(errList, fmt.Errorf("Falha no rollback de guarda: %w", rollbackErr))
		}
	} /* else {
		if commitErr := savePoint.Commit(ctx); commitErr != nil {
			errList = append(errList, fmt.Errorf("Falha no commit de guarda: %w", commitErr))
		}
	}*/

	if len(errList) > 0 {
		errList = append(errList, fmt.Errorf("For sql: %s", sql))
		return nil, errors.Join(errList...)
	}
	return &guardedRows{
		Rows:      rows,
		ctx:       ctx,
		tx:        d.tx,
		savePoint: savePoint,
	}, nil
}

func (d *realSessionDB) SafeExec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	savePoint, execErr := d.tx.Begin(ctx)
	if execErr != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, pro sql '''%s'''", execErr, sql)
	}
	result, execErr := savePoint.Exec(ctx, sql, args...)
	if execErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", execErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Safe exec failed: %w, sql: '''%s'''", execErr, sql)
	}
	if commitErr := savePoint.Commit(ctx); commitErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", commitErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Falha no commit de guarda: %w, sql: '''%s'''", commitErr, sql)
	}
	return result, nil
}

// SafeExecTCL runs all TCL (SAVEPOINT, RELEASE, ROLLBACK, ROLLBACK TO SAVEPOINT). SAVEPOINT
// must run on the main tx so the created savepoint is visible for later ROLLBACK/RELEASE;
// RELEASE and ROLLBACK run inside a guard so a failure does not abort the main transaction.
func (d *realSessionDB) SafeExecTCL(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if isSavepointCommand(sql) {
		return d.tx.Exec(ctx, sql, args...)
	}
	savePoint, execErr := d.tx.Begin(ctx)
	if execErr != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, pro sql '''%s'''", execErr, sql)
	}
	result, execErr := savePoint.Exec(ctx, sql, args...)
	if execErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", execErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Safe exec failed: %w, sql: '''%s'''", execErr, sql)
	}
	if commandInvalidatesGuardOnSuccess(sql) {
		return result, nil
	}
	if commitErr := savePoint.Commit(ctx); commitErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", commitErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Falha no commit de guarda: %w, sql: '''%s'''", commitErr, sql)
	}
	return result, nil
}

// isSavepointCommand returns true for SAVEPOINT <name>. Those must run on the main tx.
func isSavepointCommand(sql string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SAVEPOINT ")
}

// commandInvalidatesGuardOnSuccess returns true when the command's success invalidates the guard
// (we must not call Commit()). ROLLBACK/ROLLBACK TO SAVEPOINT roll back past the guard;
// RELEASE SAVEPOINT releases a savepoint that was created before the guard, which can merge
// the guard's scope and leave the guard non-existent.
func commandInvalidatesGuardOnSuccess(sql string) bool {
	s := strings.ToUpper(strings.TrimSpace(sql))
	return s == "ROLLBACK" ||
		strings.HasPrefix(s, "ROLLBACK TO SAVEPOINT") ||
		strings.HasPrefix(s, "RELEASE SAVEPOINT")
}

// HasActiveTransaction returns whether there is an active transaction (for status/reporting).
// Exported for tests and callers that need to check session state.
func (d *realSessionDB) HasActiveTransaction() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tx != nil
}

// beginTx starts a new transaction on the connection. Idempotent if already in a transaction (no-op).
func (d *realSessionDB) beginTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil {
		return nil // unit test: no real connection
	}
	if d.tx != nil {
		return nil // already in a transaction
	}
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	d.tx = tx
	return nil
}

// rollbackTx rolls back the current transaction and clears it. Safe to call if tx is nil.
func (d *realSessionDB) rollbackTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tx == nil {
		return nil
	}
	err := d.tx.Rollback(ctx)
	d.tx = nil
	return err
}

// startNewTx runs ROLLBACK on the connection (to clear any failed state) and begins a new transaction.
// Used by "pgtest rollback" to get a clean transaction.
func (d *realSessionDB) startNewTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn.PgConn().SyncConn(ctx)
	if d.conn == nil {
		return nil
	}
	if d.tx != nil {
		err := d.tx.Rollback(ctx)
		if err != nil {
			logIfVerbose("Failed to rollback on starting a new Tx: %s", err)
		}
		d.tx = nil
	}
	_, err := d.conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		return err
	}
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin new transaction: %w", err)
	}
	d.tx = tx
	return nil
}

// close rolls back the current transaction (if any), stops keepalive, and closes the connection.
func (d *realSessionDB) close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopKeepalive != nil {
		d.stopKeepalive()
		d.stopKeepalive = nil
	}
	if d.tx != nil {
		_ = d.tx.Rollback(ctx)
		d.tx = nil
	}
	if d.conn != nil {
		err := d.conn.Close(ctx)
		d.conn = nil
		return err
	}
	return nil
}

// startKeepalive starts a goroutine that pings the connection at the given interval (uses conn only for Ping).
func (d *realSessionDB) startKeepalive(interval time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil || interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ticker := time.NewTicker(interval * 10000) //Danilo só pra n chamar isso mais
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 20*time.Second)
				d.mu.Lock()
				_ = d.conn.Ping(pingCtx)
				d.mu.Unlock()
				pingCancel()
			}
		}
	}()
	d.stopKeepalive = func() {
		cancel()
		<-done
	}
}

// acquireAdvisoryLock runs pg_advisory_lock on the connection (outside tx, for session-level locking).
func (d *realSessionDB) acquireAdvisoryLock(ctx context.Context, lockKey int64) error {
	d.mu.RLock()
	conn := d.conn
	d.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey)
	return err
}

// releaseAdvisoryLock runs pg_advisory_unlock on the connection (outside tx).
func (d *realSessionDB) releaseAdvisoryLock(ctx context.Context, lockKey int64) error {
	d.mu.RLock()
	conn := d.conn
	d.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey)
	return err
}

// PgConn returns the underlying PgConn for advanced use (e.g. multi-statement batch with MultiResultReader).
// Exported for query_handler batch path and tests. Prefer Query/Exec for normal operations.
func (d *realSessionDB) PgConn() *pgconn.PgConn {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.conn == nil {
		return nil
	}
	return d.conn.PgConn()
}

// Tx returns the current transaction for advanced/test use (e.g. testutil helpers that expect pgx.Tx).
// Exported for tests. Prefer Query/Exec for normal operations.
func (d *realSessionDB) Tx() pgx.Tx {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tx
}

// newSessionDB creates a realSessionDB with the given connection and transaction (caller must have begun tx on conn).
func newSessionDB(conn *pgx.Conn, tx pgx.Tx) *realSessionDB {
	d := &realSessionDB{
		conn:               conn,
		tx:                 tx,
		preparedStatements: make(map[string]string),
		portalToStatement:  make(map[string]string),
	}
	return d
}
