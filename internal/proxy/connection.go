package proxy

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v5/pgproto3"
)

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

// sessionProvider provides session lookup and query interception by testID.
// Implemented by *Server; the connection uses it with its testID to reach the TestSession.
type sessionProvider interface {
	GetSession(testID string) *TestSession
	InterceptQuery(testID string, query string) (string, error)
}

// proxyConnection gerencia uma conexão proxy entre cliente e PostgreSQL
// Intercepta apenas SQL (Query) para modificar, mantendo resto transparente
type proxyConnection struct {
	clientConn net.Conn
	backend    *pgproto3.Backend
	testID     string
	provider   sessionProvider // used with testID to get session / intercept queries
	lastQuery  string          // Última query parseada (para conversão Parse->Execute em Query simples)
	//inExtendedQuery bool   // Indica se estamos processando Extended Query (Parse/Bind/Execute)
	mu sync.Mutex
}

// getSession returns the TestSession for this connection's testID (no parameters needed).
func (p *proxyConnection) getSession() *TestSession {
	return p.provider.GetSession(p.testID)
}

// interceptQuery runs query interception for this connection's session (uses p.testID internally).
func (p *proxyConnection) interceptQuery(query string) (string, error) {
	return p.provider.InterceptQuery(p.testID, query)
}

// startProxy inicia o proxy usando a sessão existente
// A sessão já tem conexão PostgreSQL autenticada e transação ativa
func (server *Server) startProxy(clientConn net.Conn, testID string, backend *pgproto3.Backend) {
	proxy := &proxyConnection{
		clientConn: clientConn,
		backend:    backend,
		testID:     testID,
		provider:   server, // Server implements sessionProvider
	}

	if err := proxy.sendInitialProtocolMessages(); err != nil {
		log.Printf("[PROXY] Failed to send initial protocol messages: %v", err)
		return
	}

	// Inicia o loop de mensagens refatorado em message_loop.go
	proxy.RunMessageLoop()
}

// sendInitialProtocolMessages envia as mensagens iniciais do protocolo PostgreSQL
func (p *proxyConnection) sendInitialProtocolMessages() error {
	p.backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
	p.backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	p.backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"})
	p.backend.Send(&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890})
	p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("failed to flush initial protocol messages: %w", err)
	}
	return nil
}
