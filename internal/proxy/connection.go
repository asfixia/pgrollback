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

// proxyConnection gerencia uma conexão proxy entre cliente e PostgreSQL
// Intercepta apenas SQL (Query) para modificar, mantendo resto transparente
type proxyConnection struct {
	clientConn net.Conn
	backend    *pgproto3.Backend
	server     *Server
	mu         sync.Mutex
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
