package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"pgrollback/pkg/protocol"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	// ConnectionTimeout é o timeout para operações de leitura/escrita nas conexões
	ConnectionTimeout = 3600 * time.Second
	// DefaultSessionTimeout é o timeout padrão para sessões se não especificado
	DefaultSessionTimeout = 24 * time.Hour
	// DefaultListenPort é a porta padrão de escuta se não especificada
	DefaultListenPort = 5433
	// portCheckTimeout é o timeout para verificar se uma porta está em uso
	portCheckTimeout = 100 * time.Millisecond
	// serverStartupCheckAttempts é o número de tentativas para verificar se o servidor está escutando
	serverStartupCheckAttempts = 20
	// serverStartupCheckInterval é o intervalo entre tentativas de verificação
	serverStartupCheckInterval = 100 * time.Millisecond
)

type Server struct {
	PgRollback *PgRollback
	listener   net.Listener
	listenHost string // actual bind host (set after Listen)
	listenPort int    // actual bind port (set after Listen; when 0 was requested, kernel assigns)
	wg         sync.WaitGroup
	startErr   error
	mu         sync.RWMutex
	// activeConns holds all accepted client connections so Stop() can close them and unblock handlers
	activeConns map[net.Conn]struct{}
	// GUI on same port: connections that look like HTTP are pushed here and served by guiHTTP
	guiCh       chan net.Conn
	guiListener *injectListener
	guiHTTP     *http.Server
}

// ListenHost returns the host the server is bound to (e.g. "127.0.0.1").
func (s *Server) ListenHost() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.listenHost }

// ListenPort returns the port the server is bound to. Useful when NewServer was called with port 0 (dynamic port).
func (s *Server) ListenPort() int { s.mu.RLock(); defer s.mu.RUnlock(); return s.listenPort }

// isPortInUse verifica se uma porta está em uso tentando conectar a ela
func isPortInUse(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), portCheckTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// NewServer cria uma nova instância do Server e inicia o servidor automaticamente
// Retorna sempre o Server, mesmo se houver erro ao iniciar
// Se listenHost for vazio, usa "localhost" como padrão
// Se listenPort for 0, binds to a dynamic port (use ListenPort() to get the actual port); otherwise uses DefaultListenPort (5433) when listenPort was not set by caller.
// Se sessionTimeout for 0, usa DefaultSessionTimeout (24h) como padrão
// When listenPort > 0, verifica se a porta está disponível antes de tentar iniciar o servidor
// Se houver erro ao iniciar, o erro é armazenado no Server e pode ser verificado com StartError()
func NewServer(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration, listenHost string, listenPort int, withGUI bool) *Server {
	// Usa valores padrão se não especificados
	if sessionTimeout <= 0 {
		sessionTimeout = DefaultSessionTimeout
	}
	useDynamicPort := (listenPort == 0)
	if !useDynamicPort && listenPort == 0 {
		listenPort = DefaultListenPort
	}
	if listenHost == "" {
		listenHost = "localhost"
	}

	pgrollback := NewPgRollback(postgresHost, postgresPort, postgresDB, postgresUser, postgresPass, timeout, sessionTimeout, keepaliveInterval)
	server := &Server{
		PgRollback:  pgrollback,
		listenHost:  listenHost,
		listenPort:  listenPort,
		activeConns: make(map[net.Conn]struct{}),
	}

	bindPort := listenPort
	if useDynamicPort {
		bindPort = 0
	}
	addr := fmt.Sprintf("%s:%d", listenHost, bindPort)
	if !useDynamicPort && isPortInUse(listenHost, listenPort) {
		server.mu.Lock()
		server.startErr = fmt.Errorf("port %s:%d is already in use. Cannot start server. Please stop any service using this port", listenHost, listenPort)
		server.mu.Unlock()
		return server
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		server.mu.Lock()
		server.startErr = fmt.Errorf("failed to listen on %s: %w", addr, err)
		server.mu.Unlock()
		return server
	}

	server.listener = listener
	if useDynamicPort {
		if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
			server.mu.Lock()
			server.listenPort = tcpAddr.Port
			if tcpAddr.IP != nil && !tcpAddr.IP.IsUnspecified() {
				server.listenHost = tcpAddr.IP.String()
			}
			server.mu.Unlock()
		}
	}

	if withGUI {
		server.guiCh = make(chan net.Conn, 32)
		server.guiListener = newInjectListenerWithChan(server.guiCh)
		server.guiHTTP = &http.Server{Handler: guiMux(server)}
		go func() {
			if err := server.guiHTTP.Serve(server.guiListener); err != nil && err != http.ErrServerClosed {
				log.Printf("[GUI] Server error: %v", err)
			}
		}()
	}

	go server.acceptConnections()

	actualHost := server.ListenHost()
	actualPort := server.ListenPort()
	if !server.waitUntilListening(actualHost, actualPort) {
		server.mu.Lock()
		server.startErr = fmt.Errorf("server failed to start listening on %s:%d after %d attempts", actualHost, actualPort, serverStartupCheckAttempts)
		server.mu.Unlock()
		return server
	}

	logIfVerbose("PgRollback server listening on %s:%d", actualHost, actualPort)
	return server
}

// waitUntilListening aguarda até que o servidor esteja realmente escutando na porta
func (s *Server) waitUntilListening(host string, port int) bool {
	for i := 0; i < serverStartupCheckAttempts; i++ {
		if isPortInUse(host, port) {
			return true
		}
		time.Sleep(serverStartupCheckInterval)
	}
	return false
}

// acceptConnections aceita conexões de clientes em loop (método privado)
//
// IMPORTANTE: Comportamento de Conexão e Reutilização
//
// O pgrollback é um servidor intermediário que fica entre o cliente e o PostgreSQL real.
// Todas as conexões são feitas ao pgrollback, não diretamente ao PostgreSQL.
//
// Reutilização de Conexões PostgreSQL por application_name:
// - Cada cliente se conecta ao pgrollback usando application_name (via parâmetro de conexão)
// - O application_name é extraído e convertido em testID (via protocol.ExtractTestID)
// - O mesmo testID sempre reutiliza a mesma conexão física ao PostgreSQL real
// - Isso é gerenciado por SessionsByTestID: cada sessão guarda sua DB (conn+tx) sob o testID
//
// Simulação de Autenticação para o Cliente:
//   - Cada cliente sempre passa pelo handshake completo de autenticação com o pgrollback
//   - O pgrollback simula a autenticação PostgreSQL (pede senha, responde AuthenticationOK)
//   - O cliente nunca sabe se está usando uma conexão PostgreSQL nova ou reutilizada
//   - Isso permite que aplicações como PHP (que abrem e fecham conexões rapidamente)
//     continuem usando a mesma transação PostgreSQL, mantendo as alterações
//
// Benefícios:
// - Transações não são perdidas quando o cliente fecha a conexão (PHP, scripts, etc.)
// - Alterações persistem entre reconexões do mesmo application_name
// - Controle total sobre quando fazer rollback (via comandos pgrollback especiais)
// - Isolamento entre diferentes testIDs (cada um tem sua própria transação)
func (s *Server) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			if s.listener == nil {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			s.mu.Lock()
			s.startErr = err
			s.mu.Unlock()
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		if s.guiCh != nil {
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			peeked := make([]byte, peekSize)
			n, peekErr := conn.Read(peeked)
			conn.SetReadDeadline(time.Time{})
			if peekErr == nil && n > 0 {
				peeked = peeked[:n]
				wrapped := newPeekedConn(conn, peeked)
				if isHTTPPeek(peeked) {
					s.guiListener.Push(wrapped)
					continue
				}
				s.wg.Add(1)
				go s.handleConnection(wrapped)
				continue
			}
			// Peek failed or no data: treat as PostgreSQL (replay nothing would be wrong, so use peeked if any)
			if n > 0 {
				wrapped := newPeekedConn(conn, peeked[:n])
				s.wg.Add(1)
				go s.handleConnection(wrapped)
			} else {
				conn.Close()
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	s.mu.Lock()
	if s.listener != nil {
		listener := s.listener
		s.listener = nil
		// Copy active connections so we can close them without holding mu (closing unblocks handlers)
		conns := make([]net.Conn, 0, len(s.activeConns))
		for c := range s.activeConns {
			conns = append(conns, c)
		}
		s.mu.Unlock()
		if err := listener.Close(); err != nil {
			return err
		}
		if s.guiListener != nil {
			_ = s.guiListener.Close()
		}
		if s.guiHTTP != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = s.guiHTTP.Shutdown(ctx)
			cancel()
		}
		for _, c := range conns {
			_ = c.Close()
		}
	} else {
		s.mu.Unlock()
	}
	s.wg.Wait()
	return nil
}

// addActiveConn records a client connection so Stop() can close it to unblock handlers.
func (s *Server) addActiveConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConns != nil {
		s.activeConns[c] = struct{}{}
	}
}

// removeActiveConn removes a client connection from the active set (e.g. when handler returns).
func (s *Server) removeActiveConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeConns, c)
}

// StartError retorna o erro de inicialização, se houver
func (s *Server) StartError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startErr
}

func (s *Server) handleConnection(clientConn net.Conn) {
	defer s.wg.Done()
	defer clientConn.Close()
	s.addActiveConn(clientConn)
	defer s.removeActiveConn(clientConn)

	// Log para rastrear conexões TCP ao pgrollback
	remoteAddr := clientConn.RemoteAddr().String()
	// Adiciona stack trace para identificar qual código está causando a conexão
	logIfVerbose("[SERVER] Nova conexão TCP recebida de %s", remoteAddr)
	defer logIfVerbose("[SERVER] Conexão TCP fechada de %s", remoteAddr)

	clientConn.SetDeadline(time.Now().Add(ConnectionTimeout))

	var length int32
	if err := binary.Read(clientConn, binary.BigEndian, &length); err != nil {
		if err != io.EOF {
			log.Printf("Error reading message length: %v", err)
		}
		return
	}

	// Verifica se é SSLRequest (length = 8)
	if length == 8 {
		var code int32
		if err := binary.Read(clientConn, binary.BigEndian, &code); err != nil {
			log.Printf("Error reading SSL request code: %v", err)
			return
		}

		if code == SSLRequestCode {
			if err := WriteSSLResponse(clientConn, false); err != nil {
				log.Printf("Error writing SSL response: %v", err)
				return
			}
			// Backend normal após tratar SSLRequest
			backend := pgproto3.NewBackend(clientConn, clientConn)
			s.processConnectionStartupMessage(backend, clientConn)
			return
		} else {
			// Reconstruir bytes lidos
			backend := s.createBackendWithPreRead(clientConn, 8, length, code)
			s.processConnectionStartupMessage(backend, clientConn)
			return
		}
		//// Length 8 was the SSL (or cancel) request; we consumed it. Next wire content is the StartupMessage.
		//// Do not feed the 8 bytes back into the backend or ReceiveStartupMessage() will misparse (length=8 + 4 bytes body) and params/testID can be wrong, breaking the handshake.
		//backend := pgproto3.NewBackend(clientConn, clientConn)
		//s.processConnectionStartupMessage(backend, clientConn)
		//return
	} else {
		// First message is the StartupMessage: we read only the 4-byte length; put it back so the backend sees length + body.
		backend := s.createBackendWithPreRead(clientConn, 4, length, 0)
		s.processConnectionStartupMessage(backend, clientConn)
		return
	}
}

// createBackendWithPreRead cria um backend reconstruindo bytes já lidos
func (s *Server) createBackendWithPreRead(clientConn net.Conn, dataSize int, length int32, code int32) *pgproto3.Backend {
	preReadData := make([]byte, dataSize)
	binary.BigEndian.PutUint32(preReadData[0:4], uint32(length))
	if dataSize == 8 {
		binary.BigEndian.PutUint32(preReadData[4:8], uint32(code))
	}
	multiReader := io.MultiReader(bytes.NewReader(preReadData), clientConn)
	return pgproto3.NewBackend(multiReader, clientConn)
}

// processConnectionStartupMessage processa a mensagem de startup do cliente e estabelece a sessão
//
// Fluxo de Autenticação:
// 1. Recebe StartupMessage do cliente (contém application_name e outros parâmetros)
// 2. Extrai o testID do application_name (via protocol.ExtractTestID)
// 3. Simula autenticação PostgreSQL para o cliente:
//   - Solicita senha (AuthenticationCleartextPassword)
//   - Recebe senha do cliente
//   - Responde AuthenticationOK
//
// 4. Obtém ou cria sessão para o testID:
//   - Se já existe sessão para este testID: reutiliza a conexão PostgreSQL existente
//   - Se não existe: cria nova conexão PostgreSQL e nova transação
//
// 5. Inicia proxy para encaminhar comandos entre cliente e PostgreSQL
//
// IMPORTANTE: O cliente sempre passa por autenticação completa, mesmo quando
// reutilizamos uma conexão PostgreSQL existente. Isso garante que o cliente
// não percebe diferença entre uma conexão nova e uma reutilizada.
func (s *Server) processConnectionStartupMessage(backend *pgproto3.Backend, clientConn net.Conn) {
	clientConn.SetDeadline(time.Now().Add(ConnectionTimeout))

	params, err := getConnectionStartupParameters(backend)
	if err != nil {
		return
	}
	// Extrai testID do application_name do cliente
	// O mesmo application_name sempre resulta no mesmo testID
	testID, err := protocol.ExtractTestID(params)
	if err != nil {
		errorBackend := pgproto3.NewBackend(clientConn, clientConn)
		sendErrorToClient(errorBackend, err.Error())
		return
	}

	// Log para identificar qual teste/código está fazendo a conexão
	// O testID identifica qual teste está conectando (ex: "test_commit_protection" = TestProtectionAgainstAccidentalCommit)
	appName := protocol.ExtractAppname(params)
	remoteAddr := clientConn.RemoteAddr().String()
	logIfVerbose("[SERVER] Conexão estabelecida - testID=%s, application_name=%s, origem=%s", testID, appName, remoteAddr)

	// Simula autenticação PostgreSQL: sempre solicita senha do cliente
	// Isso garante que o cliente sempre passa pelo mesmo fluxo, independente
	// de estarmos reutilizando uma conexão PostgreSQL ou criando nova
	if err := WriteAuthenticationCleartextPassword(clientConn); err != nil {
		log.Printf("Error writing authentication request: %v", err)
		return
	}

	passwordMsg, err := backend.Receive()
	if err != nil {
		log.Printf("Error receiving password message: %v", err)
		return
	}

	if _, ok := passwordMsg.(*pgproto3.PasswordMessage); !ok {
		log.Printf("Expected password message, got: %T", passwordMsg)
		return
	}

	// Obtém ou cria sessão para este testID
	// - Se já existe: reutiliza conexão PostgreSQL e transação existentes
	// - Se não existe: cria nova conexão PostgreSQL e nova transação
	// A conexão PostgreSQL é persistente e reutilizada para o mesmo testID
	_, err = s.PgRollback.GetOrCreateSession(testID)
	if err != nil {
		errorBackend := pgproto3.NewBackend(clientConn, clientConn)
		sendErrorToClient(errorBackend, err.Error())
		return
	}

	// Responde AuthenticationOK ao cliente
	// O cliente agora está "autenticado" e não sabe se estamos usando
	// uma conexão PostgreSQL nova ou reutilizada
	if err := WriteAuthenticationOK(clientConn); err != nil {
		log.Printf("Error writing authentication OK: %v", err)
		return
	}

	// Inicia proxy para encaminhar comandos entre cliente e PostgreSQL
	s.startProxy(testID, clientConn, backend)
}

func getConnectionStartupParameters(backend *pgproto3.Backend) (map[string]string, error) {
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("Error receiving startup message from client: %v", err)
	}

	params := make(map[string]string)
	if sm, ok := startupMsg.(*pgproto3.StartupMessage); ok {
		for k, v := range sm.Parameters {
			params[k] = v
		}
	}
	return params, nil
}
