package proto

import (
	"encoding/binary"
	"fmt"
	"io"

	uuid "github.com/satori/go.uuid"
)

func readSize(r io.Reader) (uint64, error) {
	var b = make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, fmt.Errorf("proto: can't read size: %s", err)
	}

	return binary.LittleEndian.Uint64(b), nil
}

func readID(r io.Reader) (uuid.UUID, error) {
	var b = make([]byte, 16)
	if _, err := io.ReadFull(r, b); err != nil {
		return uuid.Nil, fmt.Errorf("proto: can't read id: %s", err)
	}

	return uuid.FromBytes(b)
}
