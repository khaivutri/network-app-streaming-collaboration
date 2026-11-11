package rudp

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestValidateNetEmConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  NetEmConfig
		wantErr bool
	}{
		{
			name: "zero values disable simulation",
		},
		{
			name: "valid profile",
			config: NetEmConfig{
				DropRate:      0.1,
				ReorderRate:   0.2,
				DuplicateRate: 0.03,
				CorruptRate:   0.01,
				BaseLatency:   5 * time.Millisecond,
				MaxJitter:     2 * time.Millisecond,
			},
		},
		{
			name: "reject rate above one",
			config: NetEmConfig{
				DropRate: 1.1,
			},
			wantErr: true,
		},
		{
			name: "reject negative latency",
			config: NetEmConfig{
				BaseLatency: -time.Millisecond,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestNetEmConnDropsAndDuplicatesOutboundDatagrams(t *testing.T) {
	inner := &recordingPacketConn{}
	dropper, err := NewNetEmConn(inner, NetEmConfig{DropRate: 1, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := dropper.WriteToUDP([]byte("dropped"), &net.UDPAddr{}); err != nil || n != len("dropped") {
		t.Fatalf("dropped write = (%d, %v)", n, err)
	}
	if len(inner.writes) != 0 {
		t.Fatal("dropper forwarded a deliberately dropped datagram")
	}
	if got := dropper.Stats().DroppedPackets; got != 1 {
		t.Fatalf("dropped packet count = %d, want 1", got)
	}
	if err := dropper.UpdateConfig(NetEmConfig{DuplicateRate: 1, Seed: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := dropper.WriteToUDP([]byte("copy"), &net.UDPAddr{}); err != nil {
		t.Fatal(err)
	}
	if len(inner.writes) != 2 || string(inner.writes[0]) != "copy" || string(inner.writes[1]) != "copy" {
		t.Fatalf("recorded writes = %q, want two copies", inner.writes)
	}
	if got := dropper.Stats().Duplicated; got != 1 {
		t.Fatalf("duplicated count = %d, want 1", got)
	}
}

type recordingPacketConn struct {
	writes [][]byte
}

func (c *recordingPacketConn) ReadFromUDP([]byte) (int, *net.UDPAddr, error) {
	return 0, nil, errors.New("not implemented")
}
func (c *recordingPacketConn) WriteToUDP(buffer []byte, _ *net.UDPAddr) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), buffer...))
	return len(buffer), nil
}
func (c *recordingPacketConn) Close() error                     { return nil }
func (c *recordingPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *recordingPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingPacketConn) SetWriteDeadline(time.Time) error { return nil }
func (c *recordingPacketConn) SetReadBuffer(int) error          { return nil }
func (c *recordingPacketConn) SetWriteBuffer(int) error         { return nil }

func TestNewNetEmConnRejectsInvalidConfig(t *testing.T) {
	_, err := NewNetEmConn(nil, NetEmConfig{DropRate: 2})
	if err == nil || errors.Is(err, ErrInvalidNetEmConfig) == false {
		t.Fatalf("NewNetEmConn() error = %v, want ErrInvalidNetEmConfig", err)
	}
}
