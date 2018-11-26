package manager

import (
	"bytes"
	"io"

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

// Jobs is a list of job data.
type Jobs []Job

// Encode jobs to bytes.
func (j Jobs) Encode() ([]byte, error) {
	var buf = bytes.NewBuffer(nil)
	if err := bin.WriteUint32(buf, uint32(len(j))); err != nil {
		return nil, err
	}

	for _, job := range j {
		data, err := job.Encode()
		if err != nil {
			return nil, err
		}

		_, _ = buf.Write(data)
	}

	return buf.Bytes(), nil
}

// Decode jobs from bytes.
func (j *Jobs) Decode(data []byte) error {
	r := bytes.NewReader(data)
	n, err := bin.ReadUint32(r)
	if err != nil {
		return err
	}

	jobs := make(Jobs, int(n))

	for i := 0; i < int(n); i++ {
		var job Job
		if err := job.Decode(r); err != nil {
			return err
		}
		jobs[i] = job
	}

	*j = jobs
	return nil
}

// Job contains basic data about a job.
type Job struct {
	ID     string
	Name   string
	Status string
}

// Encode job data to bytes.
func (j *Job) Encode() ([]byte, error) {
	var buf = bytes.NewBuffer(nil)
	if err := bin.WriteString(buf, j.ID); err != nil {
		return nil, err
	}

	if err := bin.WriteString(buf, j.Name); err != nil {
		return nil, err
	}

	if err := bin.WriteString(buf, j.Status); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode job data from a reader.
func (j *Job) Decode(r io.Reader) error {
	var err error

	if j.ID, err = bin.ReadString(r); err != nil {
		return err
	}

	if j.Name, err = bin.ReadString(r); err != nil {
		return err
	}

	if j.Status, err = bin.ReadString(r); err != nil {
		return err
	}

	return nil
}

// JobStats contains stats about a job.
type JobStats struct {
	ID              string
	Status          string
	Name            string
	Errors          uint32
	AccumulatorSize uint32
	Tasks           struct {
		Total     uint32
		Failed    uint32
		Processed uint32
		Running   uint32
	}
}

// Encode the job stats into bytes.
func (s *JobStats) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	strings := []string{
		s.ID,
		s.Name,
		s.Status,
	}
	ints := []uint32{
		s.Errors,
		s.AccumulatorSize,
		s.Tasks.Failed,
		s.Tasks.Processed,
		s.Tasks.Running,
		s.Tasks.Total,
	}

	for _, s := range strings {
		if err := bin.WriteString(buf, s); err != nil {
			return nil, err
		}
	}

	for _, i := range ints {
		if err := bin.WriteUint32(buf, i); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Decode the job stats from bytes.
func (s *JobStats) Decode(data []byte) error {
	buf := bytes.NewReader(data)

	strings := []*string{
		&s.ID,
		&s.Name,
		&s.Status,
	}
	ints := []*uint32{
		&s.Errors,
		&s.AccumulatorSize,
		&s.Tasks.Failed,
		&s.Tasks.Processed,
		&s.Tasks.Running,
		&s.Tasks.Total,
	}

	for _, s := range strings {
		val, err := bin.ReadString(buf)
		if err != nil {
			return err
		}
		*s = val
	}

	for _, i := range ints {
		val, err := bin.ReadUint32(buf)
		if err != nil {
			return err
		}
		*i = val
	}

	return nil
}
