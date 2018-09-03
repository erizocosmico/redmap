package bin

import (
	"encoding/binary"
	"io"
)

// WriteString writes the string size as uint32 to the writer and then the
// string itself as bytes.
func WriteString(w io.Writer, s string) error {
	return WriteBytes(w, []byte(s))
}

// ReadString reads 4 bytes for the string size and then the string itself
// from the given reader.
func ReadString(r io.Reader) (string, error) {
	b, err := ReadBytes(r)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// WriteUint32 writes an uint32 to the given writer.
func WriteUint32(w io.Writer, n uint32) error {
	var b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	_, err := w.Write(b)
	return err
}

// ReadUint32 reads an uint32 from the given reader.
func ReadUint32(r io.Reader) (uint32, error) {
	var b = make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(b), nil
}

// WriteUint16 writes an uint16 to the given writer.
func WriteUint16(w io.Writer, n uint16) error {
	var b = make([]byte, 2)
	binary.LittleEndian.PutUint16(b, n)
	_, err := w.Write(b)
	return err
}

// ReadUint16 reads an uint16 from the given reader.
func ReadUint16(r io.Reader) (uint16, error) {
	var b = make([]byte, 2)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint16(b), nil
}

// WriteBytes writes the bytes size as uint32 to the writer and then the
// bytes themselves as bytes.
func WriteBytes(w io.Writer, b []byte) error {
	var sz = make([]byte, 4)
	binary.LittleEndian.PutUint32(sz, uint32(len(b)))
	if _, err := w.Write(sz); err != nil {
		return err
	}

	_, err := w.Write(b)
	return err
}

// ReadBytes reads 4 bytes for the bytes size and then the bytes themselves
// from the given reader.
func ReadBytes(r io.Reader) ([]byte, error) {
	var sz = make([]byte, 4)
	if _, err := io.ReadFull(r, sz); err != nil {
		return nil, err
	}

	b := make([]byte, int(binary.LittleEndian.Uint32(sz)))
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}

	return b, nil
}
