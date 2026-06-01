package apdu

import (
	"errors"
	"testing"
)

func TestTransmitterCloseDisconnectsAfterCloseLogicalChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		closeLogicalChannelErr error
		disconnectErr          error
		wantErrs               []error
	}{
		{
			name: "close logical channel and disconnect succeed",
		},
		{
			name:                   "close logical channel fails",
			closeLogicalChannelErr: errCloseLogicalChannel,
			wantErrs:               []error{errCloseLogicalChannel},
		},
		{
			name:          "disconnect fails",
			disconnectErr: errDisconnect,
			wantErrs:      []error{errDisconnect},
		},
		{
			name:                   "both cleanup stages fail",
			closeLogicalChannelErr: errCloseLogicalChannel,
			disconnectErr:          errDisconnect,
			wantErrs:               []error{errCloseLogicalChannel, errDisconnect},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			channel := &fakeSmartCardChannel{
				closeLogicalChannelErr: tt.closeLogicalChannelErr,
				disconnectErr:          tt.disconnectErr,
			}
			transmitter := &Transmitter{channel: channel, logicalChannel: 3}

			err := transmitter.Close()
			for _, wantErr := range tt.wantErrs {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Close() error = %v, want %v", err, wantErr)
				}
			}
			if len(tt.wantErrs) == 0 && err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}
			if channel.closedChannel != 3 {
				t.Fatalf("closed channel = %d, want 3", channel.closedChannel)
			}
			if channel.disconnects != 1 {
				t.Fatalf("disconnects = %d, want 1", channel.disconnects)
			}
		})
	}
}

var (
	errCloseLogicalChannel = errors.New("close logical channel")
	errDisconnect          = errors.New("disconnect")
)

type fakeSmartCardChannel struct {
	closeLogicalChannelErr error
	disconnectErr          error
	closedChannel          byte
	disconnects            int
}

func (f *fakeSmartCardChannel) Connect() error {
	return nil
}

func (f *fakeSmartCardChannel) Disconnect() error {
	f.disconnects++
	return f.disconnectErr
}

func (f *fakeSmartCardChannel) OpenLogicalChannel([]byte) (byte, error) {
	return 0, nil
}

func (f *fakeSmartCardChannel) Transmit([]byte) ([]byte, error) {
	return nil, nil
}

func (f *fakeSmartCardChannel) CloseLogicalChannel(channel byte) error {
	f.closedChannel = channel
	return f.closeLogicalChannelErr
}
