package proxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/jackc/pgproto3/v3"
	"github.com/pgtest/pgtest/pkg/protocol"
)

type Server struct {
	pgtest   *PGTest
	listener net.Listener
	wg       sync.WaitGroup
}

func NewServer(pgtest *PGTest) *Server {
	return &Server{
		pgtest: pgtest,
	}
}

func (s *Server) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	log.Printf("PGTest server listening on %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

func (s *Server) handleConnection(clientConn net.Conn) {
	defer s.wg.Done()
	defer clientConn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)

	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Printf("Error receiving startup message: %v", err)
		return
	}

	params := make(map[string]string)
	if sm, ok := startupMsg.(*pgproto3.StartupMessage); ok {
		for k, v := range sm.Parameters {
			params[k] = v
		}
	}

	testID, err := protocol.ExtractTestID(params)
	if err != nil {
		log.Printf("Failed to extract test ID: %v", err)
		if err := WriteErrorResponse(clientConn, err.Error()); err != nil {
			log.Printf("Error writing error response: %v", err)
		}
		return
	}

	session, err := s.pgtest.GetOrCreateSession(testID)
	if err != nil {
		log.Printf("Failed to get or create session: %v", err)
		if err := WriteErrorResponse(clientConn, err.Error()); err != nil {
			log.Printf("Error writing error response: %v", err)
		}
		return
	}

	if err := WriteAuthenticationOK(clientConn); err != nil {
		log.Printf("Error writing authentication OK: %v", err)
		return
	}

	if err := WriteParameterStatus(clientConn, "server_version", "14.0"); err != nil {
		log.Printf("Error writing parameter status: %v", err)
	}

	if err := WriteParameterStatus(clientConn, "application_name", "pgtest-proxy"); err != nil {
		log.Printf("Error writing parameter status: %v", err)
	}

	if err := WriteReadyForQuery(clientConn); err != nil {
		log.Printf("Error writing ready for query: %v", err)
		return
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Error receiving message: %v", err)
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			if err := s.handleQuery(clientConn, session, msg.String); err != nil {
				log.Printf("Error handling query: %v", err)
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			log.Printf("Unhandled message type: %T", msg)
		}
	}
}

func WriteParameterStatus(writer io.Writer, name, value string) error {
	msgBytes := []byte(name)
	msgBytes = append(msgBytes, 0)
	msgBytes = append(msgBytes, []byte(value)...)
	msgBytes = append(msgBytes, 0)
	
	length := 4 + len(msgBytes)
	response := make([]byte, 0, length+1)
	response = append(response, 'S')
	response = append(response, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	response = append(response, msgBytes...)

	_, err := writer.Write(response)
	return err
}
