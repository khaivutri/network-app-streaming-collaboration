package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// ConcurrentSimulatedClient describes one client in a concurrent UDP load run.
type ConcurrentSimulatedClient struct {
	ServerAddr string
	Username   string
	Password   string
	Timeout    time.Duration
}

// ConcurrentRunResult contains the outcome of one simulated connection.
type ConcurrentRunResult struct {
	Index         int
	ConnectionID  uint32
	AuthCode      string
	Authenticated bool
	Err           error
}

// RunConcurrentClients opens count independent UDP sockets and runs them in parallel.
func RunConcurrentClients(ctx context.Context, serverAddr string, count int, username, password string) []ConcurrentRunResult {
	if count < 0 {
		count = 0
	}
	results := make([]ConcurrentRunResult, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			worker := ConcurrentSimulatedClient{ServerAddr: serverAddr, Username: username, Password: password, Timeout: 3 * time.Second}
			results[index] = worker.Run(ctx, index)
		}(i)
	}
	wg.Wait()
	return results
}

// Run performs SYN/SYN-ACK and authentication for one simulated client.
func (c ConcurrentSimulatedClient) Run(ctx context.Context, index int) ConcurrentRunResult {
	result := ConcurrentRunResult{Index: index}
	addr, err := net.ResolveUDPAddr("udp", c.ServerAddr)
	if err != nil {
		result.Err = err
		return result
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer conn.Close()
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetReadDeadline(deadline)
	syn, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, WindowSize: protocol.DefaultWindowSize}})
	if err != nil {
		result.Err = err
		return result
	}
	if _, err = conn.WriteToUDP(syn, addr); err != nil {
		result.Err = err
		return result
	}
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		result.Err = err
		return result
	}
	ack, err := protocol.UnmarshalPacket(buf[:n])
	if err != nil {
		result.Err = err
		return result
	}
	result.ConnectionID = ack.Hdr.ConnectionID
	payload, err := protocol.EncodeJSONPayload(protocol.AuthRequestPayload{RequestID: fmt.Sprintf("concurrent-auth-%d", index), Username: c.Username, Password: c.Password, ClientVersion: protocol.DefaultClientVersion, Timestamp: time.Now().UnixMilli()})
	if err != nil {
		result.Err = err
		return result
	}
	auth, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL | protocol.FlagORD, MsgType: protocol.MsgAuthRequest, StreamID: protocol.StreamControl, ConnectionID: result.ConnectionID, WindowSize: protocol.DefaultWindowSize}, Payload: payload})
	if err != nil {
		result.Err = err
		return result
	}
	if _, err = conn.WriteToUDP(auth, addr); err != nil {
		result.Err = err
		return result
	}
	for {
		n, _, err = conn.ReadFromUDP(buf)
		if err != nil {
			result.Err = err
			return result
		}
		packet, parseErr := protocol.UnmarshalPacket(buf[:n])
		if parseErr != nil || packet.Hdr.MsgType != protocol.MsgAuthResponse {
			continue
		}
		response, decodeErr := protocol.DecodeJSONPayload[protocol.AuthResponsePayload](packet.Payload)
		if decodeErr != nil {
			result.Err = decodeErr
			return result
		}
		result.AuthCode = response.Code
		result.Authenticated = response.Success
		return result
	}
}
