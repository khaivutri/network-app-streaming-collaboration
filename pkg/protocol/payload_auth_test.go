package protocol

import "testing"

func TestAuthPayloadRoundTrip(t *testing.T) {
	want := AuthResponsePayload{
		RequestID:   "r1",
		Success:     true,
		Code:        AuthCodeOK,
		OnlinePeers: []PeerDTO{{Username: "Alice", Status: "ONLINE"}},
	}
	raw, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[AuthResponsePayload](raw)
	if err != nil || got.Code != want.Code || len(got.OnlinePeers) != 1 {
		t.Fatalf("round trip failed: %#v %v", got, err)
	}
}
