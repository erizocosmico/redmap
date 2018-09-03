package worker

import (
	"bytes"

	"github.com/erizocosmico/redmap/internal/bin"
)

// Info contains the server information.
type Info struct {
	// Version of the server.
	Version string
	// Addr of the server.
	Addr string
	// Proto version.
	Proto         uint16
	ActiveJobs    uint32
	InstalledJobs uint32
}

// Encode and write the info at the given writer.
func (i Info) Encode() ([]byte, error) {
	var w = bytes.NewBuffer(nil)
	if err := bin.WriteString(w, i.Version); err != nil {
		return nil, err
	}

	if err := bin.WriteString(w, i.Addr); err != nil {
		return nil, err
	}

	if err := bin.WriteUint16(w, i.Proto); err != nil {
		return nil, err
	}

	if err := bin.WriteUint32(w, i.ActiveJobs); err != nil {
		return nil, err
	}

	if err := bin.WriteUint32(w, i.InstalledJobs); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

// Decode the info from the given reader.
func (i *Info) Decode(data []byte) error {
	r := bytes.NewReader(data)
	var err error
	i.Version, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	i.Addr, err = bin.ReadString(r)
	if err != nil {
		return err
	}

	i.Proto, err = bin.ReadUint16(r)
	if err != nil {
		return err
	}

	i.ActiveJobs, err = bin.ReadUint32(r)
	if err != nil {
		return err
	}

	i.InstalledJobs, err = bin.ReadUint32(r)
	if err != nil {
		return err
	}

	return nil
}
