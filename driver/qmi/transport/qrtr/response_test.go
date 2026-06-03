package qrtr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/damonto/euicc-go/driver/qmi/protocol"
)

func TestResponseRejectsShortHeader(t *testing.T) {
	var response Response
	err := response.UnmarshalBinary([]byte{0x02, 0x01, 0x00, 0x3b, 0x00, 0x00})
	if err == nil || !strings.Contains(err.Error(), "data too short") {
		t.Fatalf("UnmarshalBinary error = %v, want short data error", err)
	}
}

func TestResponseRejectsTLVLengthMismatch(t *testing.T) {
	packet := encodeResponse(t, 42, protocol.QMIUIMSendAPDU, 0xAA)
	packet[5]++

	var response Response
	err := response.UnmarshalBinary(packet)
	if err == nil || !strings.Contains(err.Error(), "QMI TLV length mismatch") {
		t.Fatalf("UnmarshalBinary error = %v, want QMI TLV length mismatch", err)
	}
}

func TestBytesRejectsOversizedMessage(t *testing.T) {
	request := &protocol.Request{
		TransactionID: 42,
		MessageID:     protocol.QMIUIMSendAPDU,
		Value: protocol.TLVs{
			{Type: 0x10, Len: protocol.MaxEncodedMessageLength, Value: bytes.Repeat([]byte{0xAA}, protocol.MaxEncodedMessageLength)},
		},
	}

	transport := &Transport{}
	if _, err := transport.bytes(request); err == nil {
		t.Fatal("bytes error = nil, want oversized message error")
	}
}

func TestReadSetsRequestDeadline(t *testing.T) {
	packet := encodeResponse(t, 42, protocol.QMIUIMSendAPDU, 0xAA)
	conn := &fakeDeadlineConn{packets: [][]byte{packet}}
	response := &captureResponse{}
	request := &protocol.Request{
		TransactionID: 42,
		MessageID:     protocol.QMIUIMSendAPDU,
		Response:      response,
		ReadTimeout:   25 * time.Millisecond,
	}

	transport := &Transport{}
	n, err := transport.Read(conn, request)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(packet) {
		t.Fatalf("Read length = %d, want %d", n, len(packet))
	}
	if len(conn.deadlines) != 2 {
		t.Fatalf("SetReadDeadline calls = %d, want 2", len(conn.deadlines))
	}
	if conn.deadlines[0].IsZero() {
		t.Fatal("first deadline is zero, want request deadline")
	}
	if conn.deadlines[0].After(time.Now().Add(request.ReadTimeout)) {
		t.Fatalf("deadline = %s, want within request timeout %s", conn.deadlines[0], request.ReadTimeout)
	}
	if !conn.deadlines[1].IsZero() {
		t.Fatalf("second deadline = %s, want cleared deadline", conn.deadlines[1])
	}
	if got, want := response.payload, []byte{0xAA}; !bytes.Equal(got, want) {
		t.Fatalf("payload = %X, want %X", got, want)
	}
}

func TestReadSkipsIndicationWithoutResultTLV(t *testing.T) {
	conn := &fakeDeadlineConn{packets: [][]byte{
		encodeMessage(t, 1, 0x0043, protocol.QMIMessageTypeIndication, protocol.TLVs{
			{Type: 0x13, Len: 4, Value: []byte{0x01, 0x00, 0x00, 0x00}},
			{Type: 0x01, Len: 1, Value: []byte{0x01}},
			{Type: 0x11, Len: 1, Value: []byte{0x02}},
		}),
		encodeMessage(t, 5, protocol.QMIUIMCloseLogicalChannel, protocol.QMIMessageTypeResponse, resultTLVs()),
	}}
	request := &protocol.Request{
		TransactionID: 5,
		MessageID:     protocol.QMIUIMCloseLogicalChannel,
		Response:      noopResponse{},
		ReadTimeout:   25 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(conn, request); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
}

func TestReadSkipsMismatchedMessageID(t *testing.T) {
	conn := &fakeDeadlineConn{packets: [][]byte{
		encodeResponse(t, 42, protocol.QMIUIMCloseLogicalChannel, 0xAA),
		encodeResponse(t, 42, protocol.QMIUIMSendAPDU, 0xBB),
	}}
	response := &captureResponse{}
	request := &protocol.Request{
		TransactionID: 42,
		MessageID:     protocol.QMIUIMSendAPDU,
		Response:      response,
		ReadTimeout:   25 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(conn, request); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if got, want := response.payload, []byte{0xBB}; !bytes.Equal(got, want) {
		t.Fatalf("payload = %X, want %X", got, want)
	}
}

type fakeDeadlineConn struct {
	packets   [][]byte
	deadline  time.Time
	deadlines []time.Time
}

func (c *fakeDeadlineConn) Read(b []byte) (int, error) {
	if c.deadline.IsZero() {
		return 0, errors.New("missing read deadline")
	}
	if len(c.packets) == 0 {
		return 0, os.ErrDeadlineExceeded
	}
	packet := c.packets[0]
	c.packets = c.packets[1:]
	return copy(b, packet), nil
}

func (c *fakeDeadlineConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *fakeDeadlineConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	c.deadlines = append(c.deadlines, t)
	return nil
}

type captureResponse struct {
	payload []byte
}

func (r *captureResponse) UnmarshalResponse(tlvs *protocol.TLVs) error {
	value, ok := tlvs.Find(0x10)
	if !ok {
		return errors.New("missing payload TLV")
	}
	r.payload = value.Value
	return nil
}

type noopResponse struct{}

func (r noopResponse) UnmarshalResponse(*protocol.TLVs) error {
	return nil
}

func encodeResponse(t *testing.T, txnID uint16, messageID protocol.MessageID, payload byte) []byte {
	return encodeMessage(t, txnID, messageID, protocol.QMIMessageTypeResponse, append(resultTLVs(), protocol.TLV{Type: 0x10, Len: 1, Value: []byte{payload}}))
}

func resultTLVs() protocol.TLVs {
	return protocol.TLVs{{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}}}
}

func encodeMessage(t *testing.T, txnID uint16, messageID protocol.MessageID, messageType protocol.MessageType, tlvs protocol.TLVs) []byte {
	t.Helper()

	value := new(bytes.Buffer)
	if _, err := tlvs.WriteTo(value); err != nil {
		t.Fatalf("write TLVs: %v", err)
	}

	packet := new(bytes.Buffer)
	mustWrite(t, packet, messageType)
	mustWrite(t, packet, txnID)
	mustWrite(t, packet, messageID)
	mustWrite(t, packet, uint16(value.Len()))
	if _, err := packet.Write(value.Bytes()); err != nil {
		t.Fatalf("write packet payload: %v", err)
	}
	return packet.Bytes()
}

func mustWrite(t *testing.T, w *bytes.Buffer, value any) {
	t.Helper()
	if err := binary.Write(w, binary.LittleEndian, value); err != nil {
		t.Fatalf("binary.Write failed: %v", err)
	}
}
