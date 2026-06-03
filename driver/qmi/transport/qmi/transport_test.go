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

type noopResponse struct{}

func (r noopResponse) UnmarshalResponse(*protocol.TLVs) error {
	return nil
}

type releaseInfoResponse struct {
	info []byte
}

func (r *releaseInfoResponse) UnmarshalResponse(tlvs *protocol.TLVs) error {
	value, ok := tlvs.Find(0x01)
	if !ok {
		return errors.New("missing release info TLV")
	}
	r.info = append([]byte(nil), value.Value...)
	return nil
}

func TestReadSkipsMismatchedResponsesForSynchronousTransport(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceUIM, 8, 42, protocol.QMIUIMSendAPDU, 0xA1))
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceUIM, 7, 41, protocol.QMIUIMSendAPDU, 0xA2))
		_, _ = server.Write(encodeResponse(t, protocol.QMIServiceUIM, 7, 42, protocol.QMIUIMCloseLogicalChannel, 0xA3))
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

func TestReadSkipsIndicationWithoutResultTLV(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeMessage(t, protocol.QMIServiceUIM, 3, 1, 0x0043, protocol.QMIMessageTypeIndication, protocol.TLVs{
			{Type: 0x13, Len: 4, Value: []byte{0x01, 0x00, 0x00, 0x00}},
			{Type: 0x01, Len: 1, Value: []byte{0x01}},
			{Type: 0x11, Len: 1, Value: []byte{0x02}},
		}))
		_, _ = server.Write(encodeMessage(t, protocol.QMIServiceUIM, 3, 5, protocol.QMIUIMCloseLogicalChannel, protocol.QMIMessageTypeResponse, resultTLVs()))
	}()

	request := &protocol.Request{
		ClientID:      3,
		TransactionID: 5,
		ServiceType:   protocol.QMIServiceUIM,
		MessageID:     protocol.QMIUIMCloseLogicalChannel,
		Response:      noopResponse{},
		ReadTimeout:   100 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(client, request); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
}

func TestReadDoesNotConsumeCloseResponseForReleaseRequest(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go func() {
		defer server.Close()
		_, _ = server.Write(encodeMessage(t, protocol.QMIServiceUIM, 3, 5, protocol.QMIUIMCloseLogicalChannel, protocol.QMIMessageTypeResponse, resultTLVs()))
		_, _ = server.Write(encodeMessage(t, protocol.QMIServiceControl, 0, 6, protocol.QMICtlCmdReleaseClientID, protocol.QMIMessageTypeResponse, protocol.TLVs{
			{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}},
			{Type: 0x01, Len: 2, Value: []byte{byte(protocol.QMIServiceUIM), 0x03}},
		}))
	}()

	response := &releaseInfoResponse{}
	request := &protocol.Request{
		ClientID:      0,
		TransactionID: 6,
		ServiceType:   protocol.QMIServiceControl,
		MessageID:     protocol.QMICtlCmdReleaseClientID,
		Response:      response,
		ReadTimeout:   100 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(client, request); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(response.info, []byte{byte(protocol.QMIServiceUIM), 0x03}) {
		t.Fatalf("release info = %X, want 0B03", response.info)
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
	return encodeMessage(t, serviceType, clientID, txnID, messageID, protocol.QMIMessageTypeResponse, append(resultTLVs(), protocol.TLV{Type: 0x10, Len: 1, Value: []byte{payload}}))
}

func resultTLVs() protocol.TLVs {
	return protocol.TLVs{{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}}}
}

func encodeMessage(t *testing.T, serviceType protocol.ServiceType, clientID uint8, txnID uint16, messageID protocol.MessageID, messageType protocol.MessageType, tlvs protocol.TLVs) []byte {
	t.Helper()

	value := new(bytes.Buffer)
	if _, err := tlvs.WriteTo(value); err != nil {
		t.Fatalf("write TLVs: %v", err)
	}

	sdu := new(bytes.Buffer)
	if serviceType == protocol.QMIServiceControl {
		if err := binary.Write(sdu, binary.LittleEndian, Header[uint8]{
			MessageType:   messageType,
			TransactionID: uint8(txnID),
			MessageID:     messageID,
			MessageLength: uint16(value.Len()),
		}); err != nil {
			t.Fatalf("write control SDU header: %v", err)
		}
	} else {
		if err := binary.Write(sdu, binary.LittleEndian, Header[uint16]{
			MessageType:   messageType,
			TransactionID: txnID,
			MessageID:     messageID,
			MessageLength: uint16(value.Len()),
		}); err != nil {
			t.Fatalf("write service SDU header: %v", err)
		}
	}
	if _, err := sdu.Write(value.Bytes()); err != nil {
		t.Fatalf("write SDU payload: %v", err)
	}

	packet := new(bytes.Buffer)
	if err := binary.Write(packet, binary.LittleEndian, QMUXHeader{
		IfType:       protocol.QMUXHeaderIfType,
		Length:       uint16(sdu.Len() + 5),
		ControlFlags: protocol.QMUXHeaderControlFlagRequest,
		ServiceType:  serviceType,
		ClientID:     clientID,
	}); err != nil {
		t.Fatalf("write QMUX header: %v", err)
	}
	if _, err := packet.Write(sdu.Bytes()); err != nil {
		t.Fatalf("write packet payload: %v", err)
	}

	return packet.Bytes()
}
