package protocol

import "testing"

func TestHeartbeatPayloadRoundTrip(t *testing.T) {
	ping := PingPayload{Seq: 14, ClientTime: 123}
	raw, err := EncodeJSONPayload(ping)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[PingPayload](raw)
	if err != nil || got != ping {
		t.Fatalf("ping round trip: got=%+v err=%v", got, err)
	}
	pong := PongPayload{Seq: 14, ClientTime: 123, ServerTime: 126}
	raw, err = EncodeJSONPayload(pong)
	if err != nil {
		t.Fatal(err)
	}
	gotPong, err := DecodeJSONPayload[PongPayload](raw)
	if err != nil || gotPong != pong {
		t.Fatalf("pong round trip: got=%+v err=%v", gotPong, err)
	}
}

func TestErrorPayloadCarriesResilienceFields(t *testing.T) {
	want := ErrorMessagePayload{Code: ErrCodeInternalServer, Message: "retry", RefID: "m1", RefMsgType: MsgMessUnicast, Recoverable: true, ServerTime: 7}
	raw, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[ErrorMessagePayload](raw)
	if err != nil || got != want {
		t.Fatalf("error payload: got=%+v err=%v", got, err)
	}
}
