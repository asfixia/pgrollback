package gui

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const shutdownTimeout = 3 * time.Second

// StartGUIServer starts an HTTP server on a separate port for the GUI.
// Prefer same-port GUI by using the proxy's inject listener so the GUI is at http://host:port/gui.
func StartGUIServer(provider SessionProvider, host string, port int) (stop func(), err error) {
	if port <= 0 {
		return func() {}, nil
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("GUI listen: %w", err)
	}
	httpServer := &http.Server{Handler: NewMux(provider)}
	var once sync.Once
	stop = func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			_ = httpServer.Shutdown(ctx)
			_ = listener.Close()
		})
	}
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("[GUI] Server error: %v", err)
		}
	}()
	log.Printf("PGTest GUI available at http://%s", addr)
	return stop, nil
}
