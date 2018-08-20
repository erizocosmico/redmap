package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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
	lastOp
)

// Request to a worker or master.
type Request struct {
	Op   Op
	ID   uuid.UUID
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
func ParseRequest(r io.Reader, maxSize uint64) (*Request, error) {
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

		if size > maxSize {
			return nil, ErrTooLarge
		}

		data = make([]byte, int(maxSize))
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("proto: can't read request data: %s", err)
		}
	case Uninstall:
		id, err = readID(r)
		if err != nil {
			return nil, err
		}
	case HealthCheck:
		// nothing to do here
	default:
		return nil, ErrInvalidOp
	}

	if err := expectEOF(r); err != nil {
		return nil, err
	}

	return &Request{Op: op, ID: id, Data: data}, nil
}

func readOp(r io.Reader) (Op, error) {
	var b = make([]byte, 2)
	if _, err := io.ReadFull(r, b); err != nil {
		return Invalid, fmt.Errorf("proto: can't read op: %s", err)
	}

	o := Op(binary.LittleEndian.Uint16(b))
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

	var op = make([]byte, 2)
	binary.LittleEndian.PutUint16(op, uint16(r.Op))
	if _, err := w.Write(op); err != nil {
		return err
	}

	switch r.Op {
	case Install, ExecMap:
		if _, err := w.Write(r.ID[:]); err != nil {
			return err
		}

		size := make([]byte, 8)
		binary.LittleEndian.PutUint64(size, uint64(len(r.Data)))
		if _, err := w.Write(size); err != nil {
			return err
		}

		if _, err := w.Write(r.Data); err != nil {
			return err
		}
	case Uninstall:
		if _, err := w.Write(r.ID[:]); err != nil {
			return err
		}
	case HealthCheck:
		// done here
	}

	return nil
}
