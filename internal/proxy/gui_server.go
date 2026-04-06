package proxy

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"
)

// samePortGUIServer serves the HTTP GUI on the same TCP port as PostgreSQL: the main
// accept loop peeks the first bytes; HTTP-looking connections are pushed here and
// served by http.Server on an injectListener.
type samePortGUIServer struct {
	inject *injectListener
	http   *http.Server
}

func newSamePortGUIServer(s *Server) *samePortGUIServer {
	ch := make(chan net.Conn, 32)
	inject := newInjectListenerWithChan(ch)
	httpSrv := &http.Server{Handler: guiMux(s)}
	g := &samePortGUIServer{inject: inject, http: httpSrv}
	go g.serve()
	return g
}

func (g *samePortGUIServer) serve() {
	if err := g.http.Serve(g.inject); err != nil && err != http.ErrServerClosed {
		log.Printf("[GUI] Server error: %v", err)
	}
}

func (g *samePortGUIServer) pushConn(c net.Conn) {
	g.inject.Push(c)
}

func (g *samePortGUIServer) shutdown() {
	if g.inject != nil {
		_ = g.inject.Close()
	}
	if g.http != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = g.http.Shutdown(ctx)
		cancel()
	}
}
