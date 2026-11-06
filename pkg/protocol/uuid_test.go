package protocol

import (
	"strings"
	"testing"
)

func TestUUIDGenerateAndParseRoundtrip(t *testing.T) {
	formatted, raw, err := GenerateUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	if len(formatted) != 36 || strings.Count(formatted, "-") != 4 {
		t.Fatalf("unexpected uuid %q", formatted)
	}
	if raw[6]&0xf0 != 0x40 || raw[8]&0xc0 != 0x80 {
		t.Fatalf("uuid is not RFC 4122 v4: %x", raw)
	}
	parsed, err := ParseUUID(formatted)
	if err != nil || parsed != raw {
		t.Fatalf("roundtrip failed: %v", err)
	}
}

func TestUUIDParseInvalidStrings(t *testing.T) {
	for _, value := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-00000000000z"} {
		if _, err := ParseUUID(value); err == nil {
			t.Fatalf("expected error for %q", value)
		}
	}
}
