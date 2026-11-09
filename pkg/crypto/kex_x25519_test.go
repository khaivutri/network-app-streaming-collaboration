package crypto

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestX25519DerivationIsDirectional(t *testing.T) {
	client, err := GenerateX25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	server, err := GenerateX25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	var clientNonce, serverNonce [16]byte
	for i := range clientNonce {
		clientNonce[i] = byte(i)
	}
	for i := range serverNonce {
		serverNonce[i] = byte(i + 16)
	}
	clientWrite, clientRead, err := DeriveSessionKeys(client.PrivateKey, server.PublicKey, clientNonce, serverNonce, 7, false)
	if err != nil {
		t.Fatal(err)
	}
	serverWrite, serverRead, err := DeriveSessionKeys(server.PrivateKey, client.PublicKey, clientNonce, serverNonce, 7, true)
	if err != nil {
		t.Fatal(err)
	}
	if clientWrite != serverRead || clientRead != serverWrite || clientWrite == clientRead {
		t.Fatal("directional key derivation invariant failed")
	}
}

func TestHKDFRFC5869Vector1(t *testing.T) {
	ikm := bytes.Repeat([]byte{0x0b}, 22)
	salt, _ := hex.DecodeString("000102030405060708090a0b0c")
	info, _ := hex.DecodeString("f0f1f2f3f4f5f6f7f8f9")
	expectedPRK, _ := hex.DecodeString("077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5")
	expectedOKM, _ := hex.DecodeString("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865")
	if got := hkdfExtract(salt, ikm); !bytes.Equal(got, expectedPRK) {
		t.Fatalf("PRK=%x", got)
	}
	if got := hkdfExpand(expectedPRK, info, 42); !bytes.Equal(got, expectedOKM) {
		t.Fatalf("OKM=%x", got)
	}
}

func TestDeriveSessionKeysDefensiveInputs(t *testing.T) {
	if _, _, err := DeriveSessionKeys(nil, [32]byte{}, [16]byte{}, [16]byte{}, 1, false); err == nil {
		t.Fatal("nil private key must fail")
	}
	client, _ := GenerateX25519KeyPair()
	if _, _, err := DeriveSessionKeys(client.PrivateKey, [32]byte{}, [16]byte{}, [16]byte{}, 1, false); err == nil {
		t.Fatal("low-order peer key must fail")
	}
}

func TestDeriveSessionKeysSeparatesConnectionIDs(t *testing.T) {
	client, _ := GenerateX25519KeyPair()
	server, _ := GenerateX25519KeyPair()
	var cnonce, snonce [16]byte
	aWrite, aRead, err := DeriveSessionKeys(client.PrivateKey, server.PublicKey, cnonce, snonce, 1001, false)
	if err != nil {
		t.Fatal(err)
	}
	bWrite, bRead, err := DeriveSessionKeys(client.PrivateKey, server.PublicKey, cnonce, snonce, 1002, false)
	if err != nil {
		t.Fatal(err)
	}
	if aWrite == bWrite || aRead == bRead {
		t.Fatal("connection ID did not separate derived keys")
	}
}
