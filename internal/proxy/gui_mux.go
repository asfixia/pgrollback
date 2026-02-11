package proxy

import (
	"bytes"
	"io"
	"log"
	"net"
	"sync"
)

const peekSize = 8

// isHTTPPeek returns true if the first bytes look like an HTTP request (GET, POST, HEAD, etc.).
func isHTTPPeek(peek []byte) bool {
	if len(peek) < 4 {
		return false
	}
	switch {
	case bytes.HasPrefix(peek, []byte("GET ")):
	case bytes.HasPrefix(peek, []byte("POST ")):
	case bytes.HasPrefix(peek, []byte("HEAD ")):
	case bytes.HasPrefix(peek, []byte("PUT ")):
	case bytes.HasPrefix(peek, []byte("DELETE ")):
	case bytes.HasPrefix(peek, []byte("OPTIONS ")):
	case bytes.HasPrefix(peek, []byte("PATCH ")):
	default:
		return false
	}
	return true
}

// peekedConn wraps a connection so the first Read() returns the peeked bytes, then the rest of the connection.
type peekedConn struct {
	net.Conn
	peek *bytes.Reader
}

func newPeekedConn(conn net.Conn, peeked []byte) *peekedConn {
	return &peekedConn{Conn: conn, peek: bytes.NewReader(peeked)}
}

func (p *peekedConn) Read(b []byte) (n int, err error) {
	if p.peek != nil && p.peek.Len() > 0 {
		n, err = p.peek.Read(b)
		if err == io.EOF {
			p.peek = nil
			err = nil
			if n > 0 {
				return n, nil
			}
		}
		if n > 0 {
			return n, err
		}
	}
	return p.Conn.Read(b)
}

// injectListener is a net.Listener whose Accept() returns connections pushed via Push().
type injectListener struct {
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func newInjectListener() *injectListener {
	return newInjectListenerWithChan(make(chan net.Conn, 32))
}

func newInjectListenerWithChan(ch chan net.Conn) *injectListener {
	return &injectListener{
		ch:   ch,
		done: make(chan struct{}),
	}
}

func (l *injectListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-l.ch:
		if !ok {
			return nil, io.EOF
		}
		return conn, nil
	case <-l.done:
		return nil, io.EOF
	}
}

func (l *injectListener) Push(conn net.Conn) {
	select {
	case l.ch <- conn:
	default:
		log.Printf("[GUI] inject channel full, closing connection")
		conn.Close()
	}
}

func (l *injectListener) Close() error {
	l.once.Do(func() { close(l.done); close(l.ch) })
	return nil
}

func (l *injectListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}
