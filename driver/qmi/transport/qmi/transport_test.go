package qmi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/damonto/euicc-go/driver/qmi/protocol"
)

type captureResponse struct {
	payload []byte
}

func (r *captureResponse) UnmarshalResponse(tlvs *protocol.TLVs) error {
	value, ok := tlvs.Find(0x10)
	if !ok {
		return errors.New("missing payload TLV")
	}
	r.payload = append([]byte(nil), value.Value...)
	return nil
}

func TestReadSkipsNonMatchingResponses(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeQMIMessage(t, testQMIPacket{
			serviceType:   protocol.QMIServiceUIM,
			clientID:      7,
			transactionID: 41,
			messageID:     protocol.QMIUIMCloseLogicalChannel,
			messageType:   protocol.QMIMessageTypeIndication,
			tlvs:          protocol.TLVs{{Type: 0x13, Len: 4, Value: []byte{0x01, 0x00, 0x00, 0x00}}},
		}))
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceControl, 0, 42, protocol.QMICtlCmdReleaseClientID, 0xAA))
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceUIM, 7, 41, protocol.QMIUIMSendAPDU, 0xAA))
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceUIM, 7, 42, protocol.QMIUIMSendAPDU, 0xBB))
	}()

	response := &captureResponse{}
	request := &protocol.Request{
		ClientID:      7,
		TransactionID: 42,
		ServiceType:   protocol.QMIServiceUIM,
		MessageID:     protocol.QMIUIMSendAPDU,
		Response:      response,
		ReadTimeout:   100 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(client, request); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(response.payload, []byte{0xBB}) {
		t.Fatalf("payload = %X, want BB", response.payload)
	}
}

func TestReadSkipsCloseResponseWhenWaitingForReleaseClientID(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	request := protocol.ReleaseClientIDRequest{
		ClientID:      3,
		TransactionID: 0x106,
		ServiceType:   protocol.QMIServiceUIM,
	}
	wireRequest := request.Request()
	wireRequest.ReadTimeout = 100 * time.Millisecond

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeQMIMessage(t, testQMIPacket{
			serviceType:   protocol.QMIServiceUIM,
			clientID:      3,
			transactionID: 5,
			messageID:     protocol.QMIUIMCloseLogicalChannel,
			messageType:   protocol.QMIMessageTypeResponse,
			tlvs:          successTLVs(),
		}))
		_, _ = server.Write(encodeQMIMessage(t, testQMIPacket{
			serviceType:   protocol.QMIServiceControl,
			clientID:      0,
			transactionID: 6,
			messageID:     protocol.QMICtlCmdReleaseClientID,
			messageType:   protocol.QMIMessageTypeResponse,
			tlvs: protocol.TLVs{
				{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}},
				{Type: 0x01, Len: 2, Value: []byte{byte(protocol.QMIServiceUIM), 0x03}},
			},
		}))
	}()

	transport := &Transport{}
	if _, err := transport.Read(client, wireRequest); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if request.Response.ClientID != 3 {
		t.Fatalf("released client ID = %d, want 3", request.Response.ClientID)
	}
}

func TestReadReturnsTargetResponseQMIError(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeQMIMessage(t, testQMIPacket{
			serviceType:   protocol.QMIServiceUIM,
			clientID:      7,
			transactionID: 42,
			messageID:     protocol.QMIUIMSendAPDU,
			messageType:   protocol.QMIMessageTypeResponse,
			tlvs: protocol.TLVs{
				{
					Type:  0x02,
					Len:   4,
					Value: []byte{byte(protocol.QMIResultFailure), 0x00, byte(protocol.QMIErrorInvalidArgument), 0x00},
				},
			},
		}))
	}()

	request := &protocol.Request{
		ClientID:      7,
		TransactionID: 42,
		ServiceType:   protocol.QMIServiceUIM,
		MessageID:     protocol.QMIUIMSendAPDU,
		Response:      &captureResponse{},
		ReadTimeout:   100 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(client, request); !errors.Is(err, protocol.QMIErrorInvalidArgument) {
		t.Fatalf("Read error = %v, want %v", err, protocol.QMIErrorInvalidArgument)
	}
}

func TestReadUsesRequestDeadlineAndClearsIt(t *testing.T) {
	stopErr := errors.New("stop read")
	conn := &fakeDeadlineNetConn{readErr: stopErr}
	request := &protocol.Request{
		Response:    &captureResponse{},
		ReadTimeout: 25 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(conn, request); !errors.Is(err, stopErr) {
		t.Fatalf("Read error = %v, want %v", err, stopErr)
	}
	if len(conn.readDeadlines) != 2 {
		t.Fatalf("SetReadDeadline calls = %d, want 2", len(conn.readDeadlines))
	}
	if conn.readDeadlines[0].IsZero() {
		t.Fatal("first deadline is zero, want request deadline")
	}
	if conn.readDeadlines[0].After(time.Now().Add(request.ReadTimeout)) {
		t.Fatalf("deadline = %s, want within request timeout %s", conn.readDeadlines[0], request.ReadTimeout)
	}
	if !conn.readDeadlines[1].IsZero() {
		t.Fatalf("second deadline = %s, want cleared deadline", conn.readDeadlines[1])
	}
}

type fakeDeadlineNetConn struct {
	readErr       error
	readDeadlines []time.Time
}

func (c *fakeDeadlineNetConn) Read([]byte) (int, error)         { return 0, c.readErr }
func (c *fakeDeadlineNetConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fakeDeadlineNetConn) Close() error                     { return nil }
func (c *fakeDeadlineNetConn) LocalAddr() net.Addr              { return &net.IPAddr{} }
func (c *fakeDeadlineNetConn) RemoteAddr() net.Addr             { return &net.IPAddr{} }
func (c *fakeDeadlineNetConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeDeadlineNetConn) SetWriteDeadline(time.Time) error { return nil }

func (c *fakeDeadlineNetConn) SetReadDeadline(t time.Time) error {
	c.readDeadlines = append(c.readDeadlines, t)
	return nil
}

func encodeResponse(t *testing.T, serviceType protocol.ServiceType, clientID uint8, txnID uint16, messageID protocol.MessageID, payload byte) []byte {
	return encodeQMIMessage(t, testQMIPacket{
		serviceType:   serviceType,
		clientID:      clientID,
		transactionID: txnID,
		messageID:     messageID,
		messageType:   protocol.QMIMessageTypeResponse,
		tlvs: append(successTLVs(), protocol.TLV{
			Type:  0x10,
			Len:   1,
			Value: []byte{payload},
		}),
	})
}

func successTLVs() protocol.TLVs {
	return protocol.TLVs{{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}}}
}

type testQMIPacket struct {
	serviceType   protocol.ServiceType
	clientID      uint8
	transactionID uint16
	messageID     protocol.MessageID
	messageType   protocol.MessageType
	tlvs          protocol.TLVs
}

func encodeQMIMessage(t *testing.T, packet testQMIPacket) []byte {
	t.Helper()

	value := new(bytes.Buffer)
	if _, err := packet.tlvs.WriteTo(value); err != nil {
		t.Fatalf("write TLVs: %v", err)
	}

	sdu := new(bytes.Buffer)
	if packet.serviceType == protocol.QMIServiceControl {
		if err := binary.Write(sdu, binary.LittleEndian, Header[uint8]{
			MessageType:   packet.messageType,
			TransactionID: uint8(packet.transactionID),
			MessageID:     packet.messageID,
			MessageLength: uint16(value.Len()),
		}); err != nil {
			t.Fatalf("write control SDU header: %v", err)
		}
	} else {
		if err := binary.Write(sdu, binary.LittleEndian, Header[uint16]{
			MessageType:   packet.messageType,
			TransactionID: packet.transactionID,
			MessageID:     packet.messageID,
			MessageLength: uint16(value.Len()),
		}); err != nil {
			t.Fatalf("write service SDU header: %v", err)
		}
	}
	if _, err := sdu.Write(value.Bytes()); err != nil {
		t.Fatalf("write SDU payload: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, QMUXHeader{
		IfType:       protocol.QMUXHeaderIfType,
		Length:       uint16(sdu.Len() + 5),
		ControlFlags: 0x80,
		ServiceType:  packet.serviceType,
		ClientID:     packet.clientID,
	}); err != nil {
		t.Fatalf("write QMUX header: %v", err)
	}
	if _, err := buf.Write(sdu.Bytes()); err != nil {
		t.Fatalf("write packet payload: %v", err)
	}

	return buf.Bytes()
}
