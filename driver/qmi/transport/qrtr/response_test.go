package qrtr

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	packet := encodeResponse(t, 42, 0xAA)
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
	conn := &fakeDeadlineConn{packet: encodeResponse(t, 42, 0xAA)}
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
	if n != len(conn.packet) {
		t.Fatalf("Read length = %d, want %d", n, len(conn.packet))
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

func TestReadReturnsTargetResponseQMIError(t *testing.T) {
	conn := &fakeDeadlineConn{packet: encodeErrorResponse(t, 42, protocol.QMIErrorInvalidArgument)}
	request := &protocol.Request{
		TransactionID: 42,
		MessageID:     protocol.QMIUIMSendAPDU,
		Response:      &captureResponse{},
		ReadTimeout:   25 * time.Millisecond,
	}

	transport := &Transport{}
	if _, err := transport.Read(conn, request); !errors.Is(err, protocol.QMIErrorInvalidArgument) {
		t.Fatalf("Read error = %v, want %v", err, protocol.QMIErrorInvalidArgument)
	}
}

func TestReadSkipsNonMatchingQRTRMessages(t *testing.T) {
	conn := &fakeSequenceConn{packets: [][]byte{
		encodePacketWithHeader(t, protocol.QMIMessageTypeIndication, 42, protocol.QMIUIMCloseLogicalChannel, protocol.TLVs{
			{Type: 0x13, Len: 4, Value: []byte{0x01, 0x00, 0x00, 0x00}},
		}),
		encodeResponse(t, 41, 0xAA),
		encodeResponse(t, 42, 0xBB),
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
	packet    []byte
	deadline  time.Time
	deadlines []time.Time
}

func (c *fakeDeadlineConn) Read(b []byte) (int, error) {
	if c.deadline.IsZero() {
		return 0, errors.New("missing read deadline")
	}
	return copy(b, c.packet), nil
}

func (c *fakeDeadlineConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *fakeDeadlineConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	c.deadlines = append(c.deadlines, t)
	return nil
}

type fakeSequenceConn struct {
	packets   [][]byte
	deadline  time.Time
	deadlines []time.Time
}

func (c *fakeSequenceConn) Read(b []byte) (int, error) {
	if c.deadline.IsZero() {
		return 0, errors.New("missing read deadline")
	}
	if len(c.packets) == 0 {
		return 0, errors.New("no packet available")
	}
	packet := c.packets[0]
	c.packets = c.packets[1:]
	return copy(b, packet), nil
}

func (c *fakeSequenceConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *fakeSequenceConn) SetReadDeadline(t time.Time) error {
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

func encodeResponse(t *testing.T, txnID uint16, payload byte) []byte {
	t.Helper()

	tlvs := protocol.TLVs{
		{Type: 0x02, Len: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}},
		{Type: 0x10, Len: 1, Value: []byte{payload}},
	}
	return encodePacket(t, txnID, tlvs)
}

func encodeErrorResponse(t *testing.T, txnID uint16, qmiErr protocol.QMIError) []byte {
	t.Helper()

	return encodePacket(t, txnID, protocol.TLVs{
		{
			Type:  0x02,
			Len:   4,
			Value: []byte{byte(protocol.QMIResultFailure), 0x00, byte(qmiErr), 0x00},
		},
	})
}

func encodePacket(t *testing.T, txnID uint16, tlvs protocol.TLVs) []byte {
	return encodePacketWithHeader(t, protocol.QMIMessageTypeResponse, txnID, protocol.QMIUIMSendAPDU, tlvs)
}

func encodePacketWithHeader(t *testing.T, messageType protocol.MessageType, txnID uint16, messageID protocol.MessageID, tlvs protocol.TLVs) []byte {
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
