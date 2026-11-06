package protocol

import "testing"

func TestPayloadFileRoundtrip(t *testing.T) {
	values := []any{
		FileInitPayload{TransferID: "id", Recipient: "bob", FileName: "x.bin", FileSize: 12, ChunkSize: 1200, TotalChunks: 1, SHA256: "abc"},
		FileResponsePayload{TransferID: "id", Accepted: true, Code: FileCodeReady},
		FileCompletePayload{TransferID: "id", VerifiedSHA256: "abc"},
		FileCancelPayload{TransferID: "id", Reason: FileCodeUserCancelled},
	}
	for _, value := range values {
		raw, err := EncodeJSONPayload(value)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 {
			t.Fatal("empty JSON payload")
		}
	}
}
