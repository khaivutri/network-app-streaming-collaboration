//go:build !windows

package rudp

import "net"

func configureUDPSocket(conn *net.UDPConn) error { return nil }
