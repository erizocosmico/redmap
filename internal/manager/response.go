package manager

import (
	"bytes"

	"github.com/erizocosmico/redmap/internal/bin"
)

// Info about the manager server.
type Info struct {
	Version string
	Address string
	Proto   uint32
	Workers []string
}

// Decode info from a slice of bytes.
func (i *Info) Decode(data []byte) error {
	var r = bytes.NewReader(data)
	var err error

	i.Version, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	i.Address, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	i.Proto, err = bin.ReadUint32(r)
	if err != nil {
		return err
	}

	size, err := bin.ReadUint32(r)
	if err != nil {
		return err
	}

	i.Workers = make([]string, size)

	for j := uint32(0); j < size; j++ {
		i.Workers[j], err = bin.ReadString(r)
		if err != nil {
			return err
		}
	}

	return nil
}

// Encode Info to a slice of bytes.
func (i Info) Encode() ([]byte, error) {
	var buf = bytes.NewBuffer(nil)

	if err := bin.WriteString(buf, i.Version); err != nil {
		return nil, err
	}

	if err := bin.WriteString(buf, i.Address); err != nil {
		return nil, err
	}

	if err := bin.WriteUint32(buf, i.Proto); err != nil {
		return nil, err
	}

	if err := bin.WriteUint32(buf, uint32(len(i.Workers))); err != nil {
		return nil, err
	}

	for _, w := range i.Workers {
		if err := bin.WriteString(buf, w); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Stats about running jobs.
type Stats struct {
	Workers struct {
		Active     uint32
		Terminated uint32
		Failing    uint32
		Total      uint32
	}

	Jobs struct {
		Completed uint32
		Running   uint32
		Failed    uint32
	}
}

// Decode stats from bytes.
func (s *Stats) Decode(data []byte) error {
	var ptrs = []*uint32{
		&s.Workers.Active,
		&s.Workers.Terminated,
		&s.Workers.Failing,
		&s.Workers.Total,
		&s.Jobs.Completed,
		&s.Jobs.Running,
		&s.Jobs.Failed,
	}

	r := bytes.NewReader(data)
	for _, p := range ptrs {
		n, err := bin.ReadUint32(r)
		if err != nil {
			return err
		}

		*p = n
	}

	return nil
}

// Encode stats to bytes.
func (s Stats) Encode() ([]byte, error) {
	var data = []uint32{
		s.Workers.Active,
		s.Workers.Terminated,
		s.Workers.Failing,
		s.Workers.Total,
		s.Jobs.Completed,
		s.Jobs.Running,
		s.Jobs.Failed,
	}

	buf := bytes.NewBuffer(nil)
	for _, d := range data {
		if err := bin.WriteUint32(buf, d); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
