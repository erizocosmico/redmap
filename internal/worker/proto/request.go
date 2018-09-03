package proto

import (
	"errors"
	"fmt"
	"io"

	"github.com/erizocosmico/redmap/internal/bin"
	uuid "github.com/satori/go.uuid"
)

// Op is the operation in a request.
type Op uint16

const (
	// Invalid operation.
	Invalid Op = iota
	// Install a plugin for a job.
	Install
	// ExecMap execute a map operation.
	ExecMap
	// HealthCheck checks the status of the system.
	HealthCheck
	// Uninstall a plugin for a job.
	Uninstall
	// Info returns the server info.
	Info
	lastOp
)

// Request to a worker.
type Request struct {
	// Op is the type of the request. Must not be empty.
	Op Op
	// ID is used to reference that a request must be performed on a specific
	// job. It may be empty.
	ID uuid.UUID
	// Data is any additional data sent along with the request. May be empty.
	Data []byte
}

var (
	// ErrInvalidOp is returned when there is an invalid operation.
	ErrInvalidOp = errors.New("invalid operation")
	// ErrTooLarge is returned when the request size is too large.
	ErrTooLarge = errors.New("request size is too large")
)

// ParseRequest parses a request from the given reader. MaxSize controls
// the maximum allowed size of the data inside the request.
func ParseRequest(r io.Reader, maxSize int32) (*Request, error) {
	op, err := readOp(r)
	if err != nil {
		return nil, err
	}

	var data []byte
	var id uuid.UUID
	switch op {
	case Install, ExecMap:
		id, err = readID(r)
		if err != nil {
			return nil, err
		}

		size, err := readSize(r)
		if err != nil {
			return nil, err
		}

		if size > uint32(maxSize) {
			return nil, ErrTooLarge
		}

		data = make([]byte, int(size))
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("proto: can't read request data: %s", err)
		}
	case Uninstall:
		id, err = readID(r)
		if err != nil {
			return nil, err
		}
	case HealthCheck, Info:
		// nothing to do here
	default:
		return nil, ErrInvalidOp
	}

	return &Request{Op: op, ID: id, Data: data}, nil
}

func readOp(r io.Reader) (Op, error) {
	n, err := bin.ReadUint16(r)
	if err != nil {
		return Invalid, fmt.Errorf("proto: can't read op: %s", err)
	}

	o := Op(n)
	if o == Invalid || o >= lastOp {
		return Invalid, fmt.Errorf("proto: invalid operation %d", o)
	}

	return o, nil
}

// WriteRequest writes the request to the given writer.
func WriteRequest(r *Request, w io.Writer) error {
	if r.Op == Invalid || r.Op >= lastOp {
		return ErrInvalidOp
	}

	if err := bin.WriteUint16(w, uint16(r.Op)); err != nil {
		return err
	}

	switch r.Op {
	case Install, ExecMap:
		if _, err := w.Write(r.ID[:]); err != nil {
			return err
		}

		if err := bin.WriteBytes(w, r.Data); err != nil {
			return err
		}
	case Uninstall:
		if _, err := w.Write(r.ID[:]); err != nil {
			return err
		}
	case HealthCheck, Info:
		// done here
	}

	return nil
}
