package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Response for a request.
type Response struct {
	Type ResponseType
	Data []byte
}

// ResponseType is the type of response.
type ResponseType uint16

const (
	// InvalidResponse is an invalid response.
	InvalidResponse ResponseType = iota
	// Ok is a successful response.
	Ok
	// Error response.
	Error
	lastResponse
)

// ErrInvalidResponse is returned when there is an invalid response.
var ErrInvalidResponse = errors.New("invalid response")

// ParseResponse parses a response from the given reader. MaxSize controls
// the maximum allowed size of the data inside the response.
func ParseResponse(r io.Reader, maxSize uint64) (*Response, error) {
	t, err := readResponseType(r)
	if err != nil {
		return nil, err
	}

	var data []byte
	switch t {
	case Ok, Error:
		size, err := readSize(r)
		if err != nil {
			return nil, err
		}

		if size > maxSize {
			return nil, ErrTooLarge
		}

		data = make([]byte, int(size))
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("proto: can't read response data: %s", err)
		}
	default:
		return nil, ErrInvalidResponse
	}

	return &Response{Type: t, Data: data}, nil
}

func readResponseType(r io.Reader) (ResponseType, error) {
	var b = make([]byte, 2)
	if _, err := io.ReadFull(r, b); err != nil {
		return InvalidResponse, fmt.Errorf("proto: can't read response type: %s", err)
	}

	t := ResponseType(binary.LittleEndian.Uint16(b))
	if t == InvalidResponse || t >= lastResponse {
		return InvalidResponse, fmt.Errorf("proto: invalid response type %d", t)
	}

	return t, nil
}

// WriteResponse writes the response to the given writer.
func WriteResponse(r *Response, w io.Writer) error {
	if r.Type == InvalidResponse || r.Type >= lastResponse {
		return ErrInvalidResponse
	}

	var t = make([]byte, 2)
	binary.LittleEndian.PutUint16(t, uint16(r.Type))
	if _, err := w.Write(t); err != nil {
		return err
	}

	switch r.Type {
	case Ok, Error:
		size := make([]byte, 8)
		binary.LittleEndian.PutUint64(size, uint64(len(r.Data)))
		if _, err := w.Write(size); err != nil {
			return err
		}

		if _, err := w.Write(r.Data); err != nil {
			return err
		}
	}

	return nil
}
