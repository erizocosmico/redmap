package proto

import (
	"fmt"
	"io"

	"github.com/erizocosmico/redmap/internal/bin"
	uuid "github.com/satori/go.uuid"
)

// Version of the protocol.
const Version = 1

func readSize(r io.Reader) (uint32, error) {
	sz, err := bin.ReadUint32(r)
	if err != nil {
		return 0, fmt.Errorf("proto: can't read size: %s", err)
	}

	return sz, nil
}

func readID(r io.Reader) (uuid.UUID, error) {
	var b = make([]byte, 16)
	if _, err := io.ReadFull(r, b); err != nil {
		return uuid.Nil, fmt.Errorf("proto: can't read id: %s", err)
	}

	return uuid.FromBytes(b)
}
