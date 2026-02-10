package proxy

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"unicode"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrNoOpenUserTransaction is returned when COMMIT or ROLLBACK is executed but there is no open user transaction on this connection.
var ErrNoOpenUserTransaction = errors.New("there is no open transaction on this connection")

const pgtestSavepointPrefix = "pgtest_v_"

// extractSavepointNameFromQuery returns the first pgtest_v_* savepoint name in query, or "" if none.
// Used to ensure we only apply TCL tracking when the query refers to the session's expected savepoint.
func extractSavepointNameFromQuery(query string) string {
	i := strings.Index(query, pgtestSavepointPrefix)
	if i < 0 {
		return ""
	}
	start := i
	i += len(pgtestSavepointPrefix)
	for i < len(query) && unicode.IsDigit(rune(query[i])) {
		i++
	}
	return query[start:i]
}

// PrintR é uma função utilitária similar ao print_r do PHP
// Imprime estruturas de dados de forma legível para debugging
func PrintR(v interface{}) string {
	return spew.Sdump(v)
}

// printR é um alias não-exportado para compatibilidade interna
func printR(v interface{}) string {
	return PrintR(v)
}

// printRLog imprime usando log.Printf (similar ao print_r do PHP)
func printRLog(format string, v interface{}) {
	log.Printf(format, printR(v))
}

// proxyConnection gerencia uma conexão proxy entre cliente e PostgreSQL
// Intercepta apenas SQL (Query) para modificar, mantendo resto transparente.
// userOpenTransactionCount tracks how many user BEGINs (converted to SAVEPOINT) have not
// been closed by COMMIT/ROLLBACK on this connection; on disconnect we roll back that many
// savepoints to match real PostgreSQL behavior (implicit rollback on disconnect).
type proxyConnection struct {
	clientConn               net.Conn
	backend                  *pgproto3.Backend
	server                   *Server
	mu                       sync.Mutex
	userOpenTransactionCount int
}

// startProxy inicia o proxy usando a sessão existente
// A sessão já tem conexão PostgreSQL autenticada e transação ativa
func (server *Server) startProxy(testID string, clientConn net.Conn, backend *pgproto3.Backend) {
	proxy := &proxyConnection{
		clientConn: clientConn,
		backend:    backend,
		server:     server,
	}

	if err := proxy.sendInitialProtocolMessages(); err != nil {
		log.Printf("[PROXY] Failed to send initial protocol messages: %v", err)
		return
	}

	// Inicia o loop de mensagens refatorado em message_loop.go
	proxy.RunMessageLoop(testID)
}

// connectionID returns an opaque id for this connection so session_db can allow nested BEGIN
// on the same connection while rejecting BEGIN from a different connection.
func (p *proxyConnection) connectionID() ConnectionID {
	return ConnectionID(uintptr(unsafe.Pointer(p)))
}

// IncrementUserOpenTransactionCount is called when a user BEGIN (converted to SAVEPOINT) is executed on this connection.
func (p *proxyConnection) IncrementUserOpenTransactionCount() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.userOpenTransactionCount++
}

// DecrementUserOpenTransactionCount is called when a user COMMIT or ROLLBACK (RELEASE or ROLLBACK TO SAVEPOINT) is executed.
// Returns ErrNoOpenUserTransaction if the count is already 0.
func (p *proxyConnection) DecrementUserOpenTransactionCount() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.userOpenTransactionCount <= 0 {
		return ErrNoOpenUserTransaction
	}
	p.userOpenTransactionCount--
	return nil
}

// GetUserOpenTransactionCount returns how many user transactions are still open on this connection (for rollback on disconnect).
func (p *proxyConnection) GetUserOpenTransactionCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.userOpenTransactionCount
}

// ApplyTCLSuccessTracking is called only after a TCL command (SAVEPOINT/RELEASE/ROLLBACK TO) has been successfully executed.
// It updates session SavepointLevel (increment on SAVEPOINT), per-connection user transaction count, and releases the
// session claim when the connection's count drops to zero. Returns ErrNoOpenUserTransaction if COMMIT/ROLLBACK is applied with count already 0.
func (p *proxyConnection) ApplyTCLSuccessTracking(query string, session *TestSession) error {
	if session == nil || session.DB == nil {
		return nil
	}
	trimmed := strings.TrimSpace(query)
	savepointName := extractSavepointNameFromQuery(trimmed)
	if savepointName == "" {
		return nil
	}

	// User BEGIN -> SAVEPOINT pgtest_v_N: only apply if the name matches the session's expected next (GetNextSavepointName).
	if strings.HasPrefix(trimmed, "SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetNextSavepointName() {
			return nil
		}
		session.DB.IncrementSavepointLevel()
		p.IncrementUserOpenTransactionCount()
		return nil
	}

	// User ROLLBACK -> "ROLLBACK TO SAVEPOINT pgtest_v_N; RELEASE SAVEPOINT pgtest_v_N": name must match current (GetSavepointName); then decrement level only now (after successful execution).
	if strings.Contains(query, "ROLLBACK TO SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetSavepointName() {
			return nil
		}
		//if err := p.DecrementUserOpenTransactionCount(); err != nil {
		//	return err
		//}
		//session.DB.DecrementSavepointLevel()
		//if p.GetUserOpenTransactionCount() == 0 {
		//	session.DB.ReleaseOpenTransaction(p.connectionID(), "")
		//}
		return nil
	}

	// User COMMIT -> "RELEASE SAVEPOINT pgtest_v_N": same as above.
	if strings.HasPrefix(trimmed, "RELEASE SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetSavepointName() {
			return nil
		}
		if err := p.DecrementUserOpenTransactionCount(); err != nil {
			return err
		}
		session.DB.DecrementSavepointLevel()
		if p.GetUserOpenTransactionCount() == 0 {
			session.DB.ReleaseOpenTransaction(p.connectionID())
		}
		return nil
	}
	return nil
}

// sendInitialProtocolMessages sends the initial PostgreSQL protocol messages to the client.
// When we have a cache from the real PostgreSQL (first connection), we replay those;
// otherwise we fall back to hardcoded defaults.
func (p *proxyConnection) sendInitialProtocolMessages() error {
	cache := p.server.Pgtest.GetBackendStartupCache()
	if cache != nil && len(cache.ParameterStatuses) > 0 {
		for i := range cache.ParameterStatuses {
			ps := &cache.ParameterStatuses[i]
			p.backend.Send(&pgproto3.ParameterStatus{Name: ps.Name, Value: ps.Value})
		}
		p.backend.Send(&pgproto3.BackendKeyData{ProcessID: cache.BackendKeyData.ProcessID, SecretKey: cache.BackendKeyData.SecretKey})
	} else {
		p.backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
		p.backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		p.backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"})
		p.backend.Send(&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890})
	}
	p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("failed to flush initial protocol messages: %w", err)
	}
	return nil
}
