//go:build !debug

package proxy

import "net"

// debugLogIncomingConn is a no-op unless built with -tags debug.
func (s *Server) debugLogIncomingConn(conn net.Conn) {}

