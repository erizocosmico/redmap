package proto

import (
	"bytes"
	"fmt"
	"io"

	"github.com/erizocosmico/redmap/internal/bin"
	uuid "github.com/satori/go.uuid"
)

// Version of the protocol.
const Version = 1

// Op is the operation to be performed in a request.
type Op uint16

const (
	// Invalid operation.
	Invalid Op = iota
	// RunJob is an operation to run a job.
	RunJob
	// Hello acts as a healthcheck and provides some info about the manager
	// server.
	Hello
	// Stats returns a series of useful statistics about running jobs.
	Stats
	// Attach a new worker to the manager.
	Attach
	// Detach a worker from the manager.
	Detach
	// Jobs on a manager.
	Jobs
	// JobStats returns a series of useful statistics about a job.
	JobStats
	lastOp
)

// Request to a manager.
type Request struct {
	Op   Op
	Data []byte
}

// JobData returns the job data for an attach or detach request or an
// error if the request is not of that type.
func (r *Request) JobData() (*JobData, error) {
	if r.Op != RunJob {
		return nil, fmt.Errorf("not a RunJob request")
	}

	var data JobData
	if err := data.Decode(r.Data); err != nil {
		return nil, err
	}

	return &data, nil
}

// WorkerData returns the worker data for an attach or detach request or
// an error if the request is not of that type.
func (r *Request) WorkerData() (*WorkerData, error) {
	if r.Op != Attach && r.Op != Detach {
		return nil, fmt.Errorf("not an Attach or Detach request")
	}

	var data WorkerData
	if err := data.Decode(r.Data); err != nil {
		return nil, err
	}

	return &data, nil
}

// ParseRequest parses a request from the given reader.
func ParseRequest(r io.Reader, maxSize uint32) (*Request, error) {
	n, err := bin.ReadUint16(r)
	if err != nil {
		return nil, NewErr(err, "unable to read op: %s", err)
	}

	op := Op(n)
	if op == Invalid || op >= lastOp {
		return nil, NewErr(err, "invalid op %d: %s", op, err)
	}

	var data []byte
	switch op {
	case RunJob, Attach, Detach:
		sz, err := bin.ReadUint32(r)
		if err != nil {
			return nil, NewErr(err, "unable to read data size: %s", err)
		}

		if sz > maxSize {
			return nil, ErrTooLarge
		}

		data = make([]byte, int(sz))
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, NewErr(err, "can't read data: %s", err)
		}
	case JobStats:
		sz, err := bin.ReadUint32(r)
		if err != nil {
			return nil, NewErr(err, "unable to read data size: %s", err)
		}

		if sz != 16 {
			return nil, fmt.Errorf("data is not a valid job id")
		}

		data = make([]byte, 16)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, NewErr(err, "can't read job id: %s", err)
		}
	}

	return &Request{Op: op, Data: data}, nil
}

// WriteRequest writes a request to the given writer.
func WriteRequest(r *Request, w io.Writer) error {
	if r.Op == Invalid || r.Op >= lastOp {
		return fmt.Errorf("invalid op: %d", r.Op)
	}

	if err := bin.WriteUint16(w, uint16(r.Op)); err != nil {
		return NewErr(err, "unable to write op: %s", err)
	}

	switch r.Op {
	case RunJob, Attach, Detach, JobStats:
		if err := bin.WriteBytes(w, r.Data); err != nil {
			return NewErr(err, "can't write data: %s", err)
		}
	}

	return nil
}

// JobData is the data for a RunJob request.
type JobData struct {
	// Name of the job.
	Name string
	// ID of the job.
	ID uuid.UUID
	// Plugin to execute the job.
	Plugin []byte
}

// Decode the job data from bytes.
func (d *JobData) Decode(b []byte) error {
	var r = bytes.NewReader(b)
	var err error
	d.Name, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	var bs [16]byte
	if _, err := io.ReadFull(r, bs[:]); err != nil {
		return err
	}

	d.ID = uuid.UUID(bs)

	// LEN(4 bytes) + NAME(LEN bytes) + ID(16 bytes) + PLUGIN
	d.Plugin = b[4+len(d.Name)+16:]
	return nil
}

// Encode the job data to bytes.
func (d JobData) Encode() ([]byte, error) {
	var w = bytes.NewBuffer(nil)
	if err := bin.WriteString(w, d.Name); err != nil {
		return nil, err
	}

	if _, err := w.Write(d.ID[:]); err != nil {
		return nil, err
	}

	if _, err := w.Write(d.Plugin); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

// WorkerData is the data for an Attach or Detach request.
type WorkerData struct {
	// Addr of the worker.
	Addr string
	// Auth is data to be used for authentication purposes.
	// Only used on Attach requests.
	Auth []byte
}

// Decode data from the given bytes.
func (d *WorkerData) Decode(b []byte) error {
	var r = bytes.NewReader(b)
	var err error
	d.Addr, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	// LEN(4 bytes) + ADDR (LEN bytes) + AUTH
	d.Auth = b[4+len(d.Addr):]
	return nil
}

// Encode the worker data to bytes.
func (d WorkerData) Encode() ([]byte, error) {
	var w = bytes.NewBuffer(nil)
	if err := bin.WriteString(w, d.Addr); err != nil {
		return nil, err
	}

	if _, err := w.Write(d.Auth); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}
