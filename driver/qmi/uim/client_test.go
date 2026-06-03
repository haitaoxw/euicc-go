package uim

import (
	"testing"

	"github.com/damonto/euicc-go/driver/qmi/protocol"
)

type stubTransport struct {
	called bool
}

func (s *stubTransport) Transmit(*protocol.Request) error {
	s.called = true
	return nil
}

type cleanupStubTransport struct {
	stubTransport
	cleanupCalled bool
	request       *protocol.Request
}

func (s *cleanupStubTransport) TransmitCleanup(request *protocol.Request) error {
	s.cleanupCalled = true
	s.request = request
	return nil
}

func TestOpenLogicalChannelRejectsOversizedAID(t *testing.T) {
	transport := &stubTransport{}
	client := &Client{Transport: transport, Slot: 1}

	_, err := client.OpenLogicalChannel(make([]byte, maxAIDLength+1))
	if err == nil {
		t.Fatal("OpenLogicalChannel error = nil, want oversized AID error")
	}
	if transport.called {
		t.Fatal("transport should not be called for invalid AID")
	}
}

func TestTransmitRejectsOversizedAPDU(t *testing.T) {
	transport := &stubTransport{}
	client := &Client{Transport: transport, Slot: 1}

	_, err := client.Transmit(make([]byte, maxTransmitAPDUCommandLength+1))
	if err == nil {
		t.Fatal("Transmit error = nil, want oversized APDU error")
	}
	if transport.called {
		t.Fatal("transport should not be called for invalid APDU")
	}
}

func TestCloseLogicalChannelUsesCleanupTransport(t *testing.T) {
	transport := &cleanupStubTransport{}
	client := &Client{Transport: transport, Slot: 2, ClientID: 7}

	if err := client.CloseLogicalChannel(3); err != nil {
		t.Fatalf("CloseLogicalChannel failed: %v", err)
	}
	if !transport.cleanupCalled {
		t.Fatal("TransmitCleanup was not called")
	}
	if transport.called {
		t.Fatal("Transmit should not be called when cleanup transport is available")
	}
	if transport.request.MessageID != protocol.QMIUIMCloseLogicalChannel {
		t.Fatalf("message ID = 0x%04X, want close logical channel", transport.request.MessageID)
	}
}
