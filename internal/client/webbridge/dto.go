package webbridge

import (
	"context"
	"os"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// SessionResponse is the stable browser bootstrap contract. Collections are
// initialized to empty values so views never receive JSON null for a list/map.
type SessionResponse struct {
	CSRFToken         string                                            `json:"csrf_token"`
	Authenticated     bool                                              `json:"authenticated"`
	ConnectionState   string                                            `json:"connection_state"`
	CurrentUser       *protocol.UserInfoDTO                             `json:"current_user"`
	OnlinePeers       []protocol.PeerDTO                                `json:"online_peers"`
	BroadcastMessages []protocol.BroadcastMessageServerPayload          `json:"broadcast_messages"`
	DirectMessages    map[string][]protocol.UnicastMessageServerPayload `json:"direct_messages"`
	DeliveryStatus    map[string]string                                 `json:"delivery_status"`
	FileTransfers     []*transfer.TransferProgress                      `json:"file_transfers"`
}

// ClientService is the use-case boundary consumed by HTTP handlers.
type ClientService interface {
	Snapshot() client.SessionSnapshot
	Store() *state.ClientStore
	Events() *events.Bus
	Login(context.Context, string, string) (client.SessionSnapshot, error)
	Logout(context.Context) error
	SendBroadcast(context.Context, string, string) error
	SendDirect(context.Context, string, string, string) error
	RefreshPeers(context.Context) error
	SendFileNamed(context.Context, string, string, string, string) (string, error)
	CancelTransfer(context.Context, string) error
	OpenCompletedFile(string) (*os.File, transfer.TransferProgress, error)
}
