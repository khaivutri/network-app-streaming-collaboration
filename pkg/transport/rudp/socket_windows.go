//go:build windows

package rudp

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

const sioUDPConnReset = syscall.IOC_IN | syscall.IOC_VENDOR | 12

func configureUDPSocket(conn *net.UDPConn) error {
	if conn == nil {
		return nil
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("rudp: get raw socket: %w", err)
	}
	var controlErr error
	err = raw.Control(func(fd uintptr) {
		var returned uint32
		var disabled uint32
		controlErr = syscall.WSAIoctl(syscall.Handle(fd), sioUDPConnReset, (*byte)(unsafe.Pointer(&disabled)), uint32(unsafe.Sizeof(disabled)), nil, 0, &returned, nil, 0)
	})
	if err != nil {
		return fmt.Errorf("rudp: control raw socket: %w", err)
	}
	return controlErr
}
