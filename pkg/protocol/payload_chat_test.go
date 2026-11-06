package protocol

import "testing"

func TestPayload_BroadcastMessage_Roundtrip(t *testing.T) {
	want := BroadcastMessageClientPayload{ClientMsgID: "c1", Content: "hello", SentAt: 1}
	data, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[BroadcastMessageClientPayload](data)
	if err != nil || got != want {
		t.Fatalf("roundtrip mismatch: got=%+v err=%v", got, err)
	}
}

func TestPayload_UnicastMessage_Roundtrip(t *testing.T) {
	want := UnicastMessageServerPayload{MsgID: "m1", ClientMsgID: "c1", Sender: "alice", Recipient: "bob", Content: "secret"}
	data, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[UnicastMessageServerPayload](data)
	if err != nil || got != want {
		t.Fatalf("roundtrip mismatch: got=%+v err=%v", got, err)
	}
}

func TestPayload_MessageDelivered_Roundtrip(t *testing.T) {
	want := MessageDeliveredPayload{MsgID: "m1", ClientMsgID: "c1", Scope: "UNICAST", Recipient: "bob", Status: "DELIVERED", DeliveredAt: 2}
	data, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[MessageDeliveredPayload](data)
	if err != nil || got != want {
		t.Fatalf("roundtrip mismatch: got=%+v err=%v", got, err)
	}
}

func TestPayload_ErrorMessagePayload_Roundtrip(t *testing.T) {
	want := ErrorMessagePayload{Code: ErrCodeMsgRecipientOffline, Message: "offline", RefID: "c1", ServerTime: 3}
	data, err := EncodeJSONPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSONPayload[ErrorMessagePayload](data)
	if err != nil || got != want {
		t.Fatalf("roundtrip mismatch: got=%+v err=%v", got, err)
	}
}
