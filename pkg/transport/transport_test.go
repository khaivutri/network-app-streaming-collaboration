package transport

import "testing"

func TestGenerateConnectionID(t *testing.T) {
	const iterations = 10000
	seen := make(map[uint32]struct{}, iterations)
	for i := 0; i < iterations; i++ {
		id, err := GenerateConnectionID()
		if err != nil {
			t.Fatalf("GenerateConnectionID failed at iteration %d: %v", i, err)
		}
		if id == 0 {
			t.Fatalf("GenerateConnectionID returned zero at iteration %d", i)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate ConnectionID generated: 0x%08X", id)
		}
		seen[id] = struct{}{}
	}
}

func TestConnStateString(t *testing.T) {
	testCases := []struct {
		state    ConnState
		expected string
	}{
		{state: StateClosed, expected: "CLOSED"},
		{state: StateSynSent, expected: "SYN_SENT"},
		{state: StateSynReceived, expected: "SYN_RECEIVED"},
		{state: StateConnected, expected: "CONNECTED"},
		{state: StateFinWait, expected: "FIN_WAIT"},
		{state: StateCloseReady, expected: "CLOSE_READY"},
		{state: ConnState(99), expected: "UNKNOWN(99)"},
	}
	for _, tc := range testCases {
		if actual := tc.state.String(); actual != tc.expected {
			t.Errorf("ConnState(%d).String() = %q, expected %q", tc.state, actual, tc.expected)
		}
	}
}

func TestCloseReasonString(t *testing.T) {
	testCases := []struct {
		reason   CloseReason
		expected string
	}{
		{reason: CloseReasonNormal, expected: "NORMAL"},
		{reason: CloseReasonHandshakeTimeout, expected: "HANDSHAKE_TIMEOUT"},
		{reason: CloseReasonPeerReset, expected: "PEER_RESET"},
		{reason: CloseReasonHeartbeatTimeout, expected: "HEARTBEAT_TIMEOUT"},
		{reason: CloseReasonCancelled, expected: "CANCELLED"},
		{reason: CloseReasonServerShutdown, expected: "SERVER_SHUTDOWN"},
		{reason: CloseReason(255), expected: "UNKNOWN(255)"},
	}
	for _, tc := range testCases {
		if actual := tc.reason.String(); actual != tc.expected {
			t.Errorf("CloseReason(%d).String() = %q, expected %q", tc.reason, actual, tc.expected)
		}
	}
}
