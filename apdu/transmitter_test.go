package apdu

import (
	"errors"
	"testing"
)

func TestTransmitterCloseDisconnectsAfterCloseLogicalChannelFailure(t *testing.T) {
	closeErr := errors.New("close logical channel")
	disconnectErr := errors.New("disconnect")
	channel := &stubSmartCardChannel{
		logicalChannel:         3,
		closeLogicalChannelErr: closeErr,
		disconnectErr:          disconnectErr,
	}
	transmitter := &Transmitter{
		channel:        channel,
		logicalChannel: 3,
	}

	err := transmitter.Close()

	if !channel.closeLogicalChannelCalled {
		t.Fatal("CloseLogicalChannel was not called")
	}
	if !channel.disconnectCalled {
		t.Fatal("Disconnect was not called")
	}
	if channel.closedChannel != 3 {
		t.Fatalf("CloseLogicalChannel channel = %d, want 3", channel.closedChannel)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want close error", err)
	}
	if !errors.Is(err, disconnectErr) {
		t.Fatalf("Close() error = %v, want disconnect error", err)
	}
}

func TestTransmitterCloseReturnsNilWhenCloseAndDisconnectSucceed(t *testing.T) {
	channel := &stubSmartCardChannel{logicalChannel: 1}
	transmitter := &Transmitter{
		channel:        channel,
		logicalChannel: 1,
	}

	if err := transmitter.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if !channel.closeLogicalChannelCalled {
		t.Fatal("CloseLogicalChannel was not called")
	}
	if !channel.disconnectCalled {
		t.Fatal("Disconnect was not called")
	}
}

type stubSmartCardChannel struct {
	logicalChannel            byte
	closedChannel             byte
	closeLogicalChannelCalled bool
	disconnectCalled          bool
	closeLogicalChannelErr    error
	disconnectErr             error
}

func (s *stubSmartCardChannel) Connect() error {
	return nil
}

func (s *stubSmartCardChannel) Disconnect() error {
	s.disconnectCalled = true
	return s.disconnectErr
}

func (s *stubSmartCardChannel) OpenLogicalChannel([]byte) (byte, error) {
	return s.logicalChannel, nil
}

func (s *stubSmartCardChannel) Transmit([]byte) ([]byte, error) {
	return nil, nil
}

func (s *stubSmartCardChannel) CloseLogicalChannel(channel byte) error {
	s.closeLogicalChannelCalled = true
	s.closedChannel = channel
	return s.closeLogicalChannelErr
}
