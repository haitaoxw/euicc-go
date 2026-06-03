package qmi

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/damonto/euicc-go/driver/qmi/protocol"
	"github.com/damonto/euicc-go/driver/qmi/uim"
)

type fakeTransport struct {
	called bool
	err    error
}

func (f *fakeTransport) Transmit(*protocol.Request) error {
	f.called = true
	return f.err
}

type fakeCleanupTransport struct {
	fakeTransport
	cleanupCalled bool
}

func (f *fakeCleanupTransport) TransmitCleanup(*protocol.Request) error {
	f.cleanupCalled = true
	return f.err
}

type fakeConn struct {
	closed   bool
	closeErr error
}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *fakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fakeConn) Close() error                     { c.closed = true; return c.closeErr }
func (c *fakeConn) LocalAddr() net.Addr              { return nil }
func (c *fakeConn) RemoteAddr() net.Addr             { return nil }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func TestDisconnectClosesConnectionWhenReleaseFails(t *testing.T) {
	releaseErr := errors.New("release client id")
	closeErr := errors.New("close conn")
	transport := &fakeTransport{err: releaseErr}
	conn := &fakeConn{closeErr: closeErr}
	q := &QMI{
		conn: conn,
		Client: uim.Client{
			Transport: transport,
			ClientID:  7,
		},
	}

	err := q.Disconnect()
	if !transport.called {
		t.Fatal("releaseClientID was not called")
	}
	if !conn.closed {
		t.Fatal("connection was not closed")
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("disconnect error %v does not include release error", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("disconnect error %v does not include close error", err)
	}
}

func TestDisconnectUsesCleanupTransportForRelease(t *testing.T) {
	transport := &fakeCleanupTransport{}
	conn := &fakeConn{}
	q := &QMI{
		conn: conn,
		Client: uim.Client{
			Transport: transport,
			ClientID:  7,
		},
	}

	if err := q.Disconnect(); err != nil {
		t.Fatalf("Disconnect failed: %v", err)
	}
	if !transport.cleanupCalled {
		t.Fatal("TransmitCleanup was not called")
	}
	if transport.called {
		t.Fatal("Transmit should not be called when cleanup transport is available")
	}
}

func TestNewRejectsInvalidInputsBeforeDial(t *testing.T) {
	if _, err := New("/dev/cdc-wdm1", 0); err == nil {
		t.Fatal("New error = nil, want invalid slot error")
	}

	if _, err := New(strings.Repeat("x", 0x10000), 1); err == nil {
		t.Fatal("New error = nil, want oversized device path error")
	}
}

func TestNewQRTRRejectsInvalidSlotBeforeSocket(t *testing.T) {
	if _, err := NewQRTR(0); err == nil {
		t.Fatal("NewQRTR error = nil, want invalid slot error")
	}
}
