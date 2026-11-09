package integration

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/webbridge"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

type step9FakeService struct {
	store *state.ClientStore
	bus   *events.Bus
}

func newStep9FakeService() *step9FakeService {
	store := state.NewClientStore()
	store.SetLoginSuccess(&protocol.UserInfoDTO{
		Username: "Alice",
	}, []protocol.PeerDTO{
		{
			Username: "Bob",
			Status:   "ONLINE",
		},
	})
	return &step9FakeService{store: store, bus: events.NewBus()}
}

func (f *step9FakeService) Snapshot() client.SessionSnapshot {
	return client.SessionSnapshot{
		Authenticated:   f.store.IsLoggedIn(),
		ConnectionState: f.store.ConnectionState(),
		CurrentUser:     f.store.CurrentUser(),
		OnlinePeers:     f.store.GetOnlinePeers(),
	}
}
func (f *step9FakeService) Store() *state.ClientStore { return f.store }
func (f *step9FakeService) Events() *events.Bus       { return f.bus }
func (f *step9FakeService) Login(context.Context, string, string) (client.SessionSnapshot, error) {
	return f.Snapshot(), nil
}
func (f *step9FakeService) Logout(context.Context) error                             { return nil }
func (f *step9FakeService) SendBroadcast(context.Context, string, string) error      { return nil }
func (f *step9FakeService) SendDirect(context.Context, string, string, string) error { return nil }
func (f *step9FakeService) RefreshPeers(context.Context) error                       { return nil }
func (f *step9FakeService) SendFileNamed(context.Context, string, string, string, string) (string, error) {
	return "transfer-id", nil
}
func (f *step9FakeService) CancelTransfer(context.Context, string) error { return nil }
func (f *step9FakeService) OpenCompletedFile(string) (*os.File, transfer.TransferProgress, error) {
	return nil, transfer.TransferProgress{}, errors.New("not found")
}

func TestStep9_HTTPBridgeSecurityAndSSE(t *testing.T) {
	service := newStep9FakeService()
	bridge, err := webbridge.NewServer(service, webbridge.Config{
		ListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionRequest := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/session", nil)
	sessionRequest.Host = "127.0.0.1"
	sessionResponse := httptest.NewRecorder()
	bridge.Handler().ServeHTTP(sessionResponse, sessionRequest)
	if sessionResponse.Code != http.StatusOK || !strings.Contains(sessionResponse.Body.String(), "csrf_token") {
		t.Fatalf("session status=%d body=%s", sessionResponse.Code, sessionResponse.Body.String())
	}
	for _, origin := range []string{"http://evil.example", "http://192.168.1.2"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/session", nil)
		request.Host = "127.0.0.1"
		request.Header.Set("Origin", origin)
		response := httptest.NewRecorder()
		bridge.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("origin=%s status=%d", origin, response.Code)
		}
	}
	service.bus.Publish("connection.status", map[string]any{"state": "CONNECTED"})
}
