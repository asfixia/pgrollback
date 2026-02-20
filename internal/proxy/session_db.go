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

	sqlpkg "pgrollback/pkg/sql"
)

// ConnectionID is an opaque identifier for a proxy connection, used to allow
// nested BEGIN (same connection) while rejecting BEGIN from a different connection.
type ConnectionID = uintptr

// ErrOnlyOneTransactionAtATime is returned when a second connection tries to BEGIN while another already has an open user transaction on the same session.
var ErrOnlyOneTransactionAtATime = errors.New("only one transaction could start a transaction at a time on our pgrollback")

// realSessionDB encapsulates the PostgreSQL connection and its active transaction.
// - The connection (conn) is only used for transaction control: Begin, Rollback base, Close, keepalive, advisory lock.
// - All data operations (Query, Exec) go through the transaction so commands are never run outside the transaction.
//
// Callers use Query/Exec; the abstraction ensures the right object (tx) is used.
// You cannot "get the transaction from Conn" in pgx—Conn.Begin() returns a Tx, so both are stored and managed here.
type realSessionDB struct {
	conn                 *pgx.Conn
	tx                   pgx.Tx
	mu                   sync.RWMutex // state + serializes SQL execution (Lock for SafeExec/SafeQuery/SafeExecTCL and PgConn().Exec)
	SavepointLevel       int
	connectionWithOpenTx ConnectionID // which connection has the transaction; 0 when none
	stopKeepalive func()
	queryHistory  []QueryHistoryEntry // last N executed queries (oldest first), max maxQueryHistory
}

func (d *realSessionDB) GetSavepointLevel() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.SavepointLevel
}

// GetSavepointName returns the name for the current savepoint level. Caller must hold d.mu when level may be changing.
func (d *realSessionDB) GetSavepointName() string {
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel)
}

// GetNextSavepointName returns the name for the next SAVEPOINT (current level + 1) without incrementing.
// Used by the interceptor so SavepointLevel is only incremented when the SAVEPOINT is actually executed.
func (d *realSessionDB) GetNextSavepointName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel+1)
}

// IncrementSavepointLevel increments the savepoint level. Call only after a SAVEPOINT has been successfully executed.
func (d *realSessionDB) IncrementSavepointLevel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.SavepointLevel++
}

// DecrementSavepointLevel decrements the savepoint level. Call only after a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT has been successfully executed. No-op if level is already 0.
func (d *realSessionDB) DecrementSavepointLevel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decrementSavepointLevelLocked()
}

// DecrementSavepointLevel decrements the savepoint level. Call only after a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT has been successfully executed. No-op if level is already 0.
func (d *realSessionDB) decrementSavepointLevelLocked() {
	if d.SavepointLevel > 0 {
		d.SavepointLevel--
	}
}

// getSavepointNameLocked returns the name for the current savepoint level. Caller must hold d.mu.
func (d *realSessionDB) getSavepointNameLocked() string {
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel)
}

// getNextSavepointNameLocked returns the name for the next SAVEPOINT (current level + 1) without incrementing. Caller must hold d.mu.
func (d *realSessionDB) getNextSavepointNameLocked() string {
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel+1)
}

// incrementSavepointLevelLocked increments the savepoint level. Caller must hold d.mu.
func (d *realSessionDB) incrementSavepointLevelLocked() {
	d.SavepointLevel++
}

// LockRun holds d.mu for the duration of using the backend outside SafeExec/SafeQuery/SafeExecTCL (e.g. PgConn().Exec). Unlock with UnlockRun.
func (d *realSessionDB) LockRun() {
	d.mu.Lock()
}

// UnlockRun releases d.mu held by LockRun.
func (d *realSessionDB) UnlockRun() {
	d.mu.Unlock()
}

// execTxLocked runs a single SQL command on d.tx. Caller must hold d.mu (e.g. via LockRun).
// Used to run SAVEPOINT/ROLLBACK TO SAVEPOINT/RELEASE SAVEPOINT around a batch without releasing the lock.
func (d *realSessionDB) execTxLocked(ctx context.Context, sql string) (pgconn.CommandTag, error) {
	if d.tx == nil {
		return pgconn.CommandTag{}, fmt.Errorf("no active transaction")
	}
	tag, err := d.tx.Exec(ctx, sql)
	return tag, err
}

// hasOpenUserTransaction returns true when any connection has an open user transaction.
// Caller must hold d.mu.
func (d *realSessionDB) hasOpenUserTransaction() bool {
	return d.connectionWithOpenTx != 0
}

// isTransactionHeldByOtherConnection returns true when a connection other than connID has the open transaction.
// Caller must hold d.mu.
func (d *realSessionDB) isTransactionHeldByOtherConnection(connID ConnectionID) bool {
	return d.hasOpenUserTransaction() && d.connectionWithOpenTx != connID
}

// IsUserBeginQuery returns true when the query is a user BEGIN (SAVEPOINT pgrollback_v_*).
// Callers use this to decide whether to call ClaimOpenTransaction (e.g. before executing TCL).
func IsUserBeginQuery(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsSavepoint(stmts[0].Stmt) && strings.HasPrefix(sqlpkg.GetSavepointName(stmts[0].Stmt), pgrollbackSavepointPrefix)
}

// isUserReleaseQuery returns true when the query is a user COMMIT (RELEASE SAVEPOINT pgrollback_v_*).
func isUserReleaseQuery(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsReleaseSavepoint(stmts[0].Stmt) && strings.HasPrefix(sqlpkg.GetSavepointName(stmts[0].Stmt), pgrollbackSavepointPrefix)
}

// IsQueryThatAffectsClaim returns true when the query is one that claimed (BEGIN) or that would release (COMMIT).
// Callers use this to decide whether to call ReleaseOpenTransaction (e.g. on TCL failure).
func IsQueryThatAffectsClaim(query string) bool {
	return IsUserBeginQuery(query) || isUserReleaseQuery(query)
}

// ClaimOpenTransaction records that the given connection is starting a user transaction (BEGIN).
// Call only when the query is a user BEGIN (e.g. when IsUserBeginQuery(query) or when applying side effects after executing SAVEPOINT in tests).
// Nested BEGIN on the same connection is allowed; returns ErrOnlyOneTransactionAtATime only
// when a different connection already has an open transaction.
func (d *realSessionDB) ClaimOpenTransaction(connID ConnectionID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isTransactionHeldByOtherConnection(connID) {
		return ErrOnlyOneTransactionAtATime
	}
	d.connectionWithOpenTx = connID
	return nil
}

// ReleaseOpenTransaction clears the "one connection has open transaction" flag when the
// given connection is the one that had the claim. Call only when the claim should be released
// (e.g. on disconnect, or when level drops to 0 after COMMIT/ROLLBACK, or on TCL failure when IsQueryThatAffectsClaim(query)).
func (d *realSessionDB) ReleaseOpenTransaction(connID ConnectionID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.releaseOpenTransactionLocked(connID)
}

func (d *realSessionDB) releaseOpenTransactionLocked(connID ConnectionID) {
	if d.connectionWithOpenTx == connID {
		d.connectionWithOpenTx = 0
	}
}

// GetLastQuery returns the last executed query for this session (for GUI/status), derived from query history.
func (d *realSessionDB) GetLastQuery() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.queryHistory) == 0 {
		return ""
	}
	return d.queryHistory[len(d.queryHistory)-1].Query
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
		// Do not decrement here; level is decremented only when the command is successfully executed (in ApplyTCLSuccessTracking).
		// Faz rollback até o savepoint e depois o remove (RELEASE)
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
		// Do not decrement here; level is decremented only when the command is successfully executed (in ApplyTCLSuccessTracking).
		return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
	}

	return DEFAULT_SELECT_ONE, nil
}

func (d *realSessionDB) handleBegin(testID string, connID ConnectionID) (string, error) {
	if !d.HasActiveTransaction() {
		return "", fmt.Errorf("no active transaction: use BeginTx first")
	}

	// When connID is set (proxy path), fail if another connection already has an open user transaction.
	if connID != 0 {
		d.mu.Lock()
		heldByOther := d.isTransactionHeldByOtherConnection(connID)
		d.mu.Unlock()
		if heldByOther {
			return "", ErrOnlyOneTransactionAtATime
		}
	}

	// Garantia de segurança: se não houver transação base, cria uma primeiro
	// Isso pode acontecer se a transação foi commitada/rollback mas a sessão ainda existe
	// Em testes unitários (session.DB == nil ou conn nil), BeginTx é no-op
	if err := d.beginTx(context.Background()); err != nil {
		return "", fmt.Errorf("Failed to Begin a transaction: %w", err)
	}

	// Single logical level: only the first BEGIN creates a savepoint. Further BEGINs are no-ops (no error).
	// COMMIT/ROLLBACK when level > 0 are "real"; when level is 0 they return success without doing anything.
	if d.SavepointLevel >= 1 {
		return DEFAULT_SELECT_ONE, nil
	}
	// Return the next savepoint name without incrementing; level is incremented only when the SAVEPOINT is successfully executed (in query_handler).
	name := d.GetNextSavepointName()
	return fmt.Sprintf("SAVEPOINT %s", name), nil
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
	d.mu.Lock()
	defer d.mu.Unlock()
	savePoint, err := d.tx.Begin(ctx)
	if err != nil || savePoint == nil {
		return nil, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, sql: '''%s'''", err, sql)
	}
	rows, err := savePoint.Query(ctx, sql, args...)
	errList := []error{}
	if err != nil {
		errList = append(errList, fmt.Errorf("Falha ao executar consulta due to: %w", err))
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
	d.mu.Lock()
	defer d.mu.Unlock()
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
	d.mu.Lock()
	defer d.mu.Unlock()
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
func isSavepointCommand(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsSavepoint(stmts[0].Stmt)
}

// commandInvalidatesGuardOnSuccess returns true when the command's success invalidates the guard
// (we must not call Commit()). ROLLBACK/ROLLBACK TO SAVEPOINT roll back past the guard;
// RELEASE SAVEPOINT releases a savepoint that was created before the guard, which can merge
// the guard's scope and leave the guard non-existent.
func commandInvalidatesGuardOnSuccess(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	stmt := stmts[0].Stmt
	return sqlpkg.IsTransactionRollback(stmt) || sqlpkg.IsRollbackToSavepoint(stmt) || sqlpkg.IsReleaseSavepoint(stmt)
}

// RollbackUserSavepointsOnDisconnect rolls back the given number of user-opened savepoints
// (from user BEGINs) without touching the base transaction. Called when a client disconnects
// so that uncommitted work is rolled back, matching real PostgreSQL behavior.
// count is the number of open user transactions on that connection (from the proxy connection's counter).
func (d *realSessionDB) RollbackUserSavepointsOnDisconnect(ctx context.Context, count int) error {
	if count <= 0 {
		return nil
	}
	for i := 0; i < count; i++ {
		d.mu.Lock()
		if d.SavepointLevel <= 0 {
			d.mu.Unlock()
			break
		}
		name := fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel)
		d.SavepointLevel--
		d.mu.Unlock()

		sql := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", name, name)
		if _, err := d.SafeExecTCL(ctx, sql); err != nil {
			logIfVerbose("[PROXY] RollbackUserSavepointsOnDisconnect: %v", err)
			return err
		}
	}
	return nil
}

// HasActiveTransaction returns whether there is an active transaction (for status/reporting).
// Exported for tests and callers that need to check session state.
func (d *realSessionDB) HasActiveTransaction() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tx != nil
}

// HasOpenUserTransaction returns true if a connection has started a user transaction (BEGIN)
// and not yet committed or rolled back. Use this for GUI/status to show "user tx open" vs internal state.
func (d *realSessionDB) HasOpenUserTransaction() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.connectionWithOpenTx != 0
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
// Used by "pgrollback rollback" to get a clean transaction.
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
	d.queryHistory = nil
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
		conn: conn,
		tx:   tx,
	}
	return d
}
