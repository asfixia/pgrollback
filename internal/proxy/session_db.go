package proxy

import (
	"context"
	"fmt"
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
	conn          *pgx.Conn
	tx            pgx.Tx
	mu            sync.RWMutex
	stopKeepalive func()
}

// Ensure realSessionDB implements pgxQueryer (used by tx_guard).
var _ pgxQueryer = (*realSessionDB)(nil)

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
	if d.conn == nil {
		return nil
	}
	if d.tx != nil {
		_ = d.tx.Rollback(ctx)
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
	ticker := time.NewTicker(interval)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = d.conn.Ping(pingCtx)
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
	d := &realSessionDB{conn: conn, tx: tx}
	return d
}
