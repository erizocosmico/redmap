package manager

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/erizocosmico/redmap/internal/bin"

	"github.com/erizocosmico/redmap/internal/manager/proto"
	"github.com/satori/go.uuid"
)

// Client executes operations on a manager. This client has a single connection
// so it's not thread-safe.
type Client struct {
	conn net.Conn
}

// ClientOptions provides configuration options for the worker client.
type ClientOptions struct {
	// WriteTimeout is the maximum time to wait for a write operation before
	// it is cancelled.
	WriteTimeout time.Duration
	// ReadTimeout is the maximum time to wait for a read operation before
	// it is cancelled.
	ReadTimeout time.Duration
}

// NewClient creates a new client to operate the manager.
// WriteTimeout and readTimeout control the time before reads and writes to
// the worker will timeout. MaxSize controls the maximum allowed size in
// response data.
func NewClient(
	addr string,
	opts *ClientOptions,
) (*Client, error) {
	var readTimeout, writeTimeout time.Duration
	if opts != nil {
		readTimeout = opts.ReadTimeout
		writeTimeout = opts.WriteTimeout
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("can't connect to manager at %s: %s", addr, err)
	}

	if readTimeout > 0 {
		err := conn.SetReadDeadline(time.Now().Add(writeTimeout + readTimeout))
		if err != nil {
			return nil, err
		}
	}

	if writeTimeout > 0 {
		err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err != nil {
			return nil, err
		}
	}

	return &Client{conn}, nil
}

// Close all the connections in the client.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) request(r *proto.Request) ([]byte, error) {
	defer c.conn.Close()

	err := proto.WriteRequest(r, c.conn)
	if err != nil {
		return nil, fmt.Errorf("could not write request: %s", err)
	}

	resp, err := proto.ParseResponse(c.conn, math.MaxInt32)
	if err != nil {
		return nil, fmt.Errorf("could not parse response: %s", err)
	}

	switch resp.Type {
	case proto.Ok:
		return resp.Data, nil
	case proto.Error:
		return nil, fmt.Errorf(string(resp.Data))
	default:
		return nil, fmt.Errorf("got unknown response from manager: %d", resp.Type)
	}
}

// ErrNotImplemented is an error returned when the operation is not implemented.
var ErrNotImplemented = errors.New("operation not implemented")

// Hello acts as a healthcheck and provides some info about the manager
// server.
func (c *Client) Hello() (*Info, error) {
	data, err := c.request(&proto.Request{
		Op: proto.Hello,
	})
	if err != nil {
		return nil, err
	}

	var info Info
	if err := info.Decode(data); err != nil {
		return nil, err
	}

	return &info, nil
}

// RunJob runs an job on the cluster.
func (c *Client) RunJob(name string, id uuid.UUID, plugin []byte) error {
	data, err := proto.JobData{
		ID:     id,
		Name:   name,
		Plugin: plugin,
	}.Encode()
	if err != nil {
		return err
	}

	_, err = c.request(&proto.Request{
		Op:   proto.RunJob,
		Data: data,
	})
	return err
}

// Stats returns a series of useful statistics about running jobs.
func (c *Client) Stats() (*Stats, error) {
	data, err := c.request(&proto.Request{
		Op: proto.Stats,
	})
	if err != nil {
		return nil, err
	}

	var stats Stats
	if err := stats.Decode(data); err != nil {
		return nil, err
	}

	return &stats, nil
}

// Attach a new worker to the manager.
func (c *Client) Attach(addr string) error {
	// TODO(erizocosmico): handle auth
	data, err := proto.WorkerData{Addr: addr}.Encode()
	if err != nil {
		return err
	}

	_, err = c.request(&proto.Request{
		Op:   proto.Attach,
		Data: data,
	})
	return err
}

// Detach a worker from the manager.
func (c *Client) Detach(addr string) error {
	data, err := proto.WorkerData{Addr: addr}.Encode()
	if err != nil {
		return err
	}

	_, err = c.request(&proto.Request{
		Op:   proto.Detach,
		Data: data,
	})
	return err
}

// Jobs returns a list of jobs in the manager.
func (c *Client) Jobs() (Jobs, error) {
	resp, err := c.request(&proto.Request{
		Op: proto.Jobs,
	})
	if err != nil {
		return nil, err
	}

	var jobs Jobs
	if err := jobs.Decode(resp); err != nil {
		return nil, err
	}

	return jobs, nil
}

// JobStats returns the stats for the given job.
func (c *Client) JobStats(id uuid.UUID) (*JobStats, error) {
	buf := bytes.NewBuffer(nil)
	if err := bin.WriteUint32(buf, 16); err != nil {
		return nil, err
	}

	_, _ = buf.Write(id[:])

	resp, err := c.request(&proto.Request{
		Op:   proto.Jobs,
		Data: buf.Bytes(),
	})
	if err != nil {
		return nil, err
	}

	var stats JobStats
	if err := stats.Decode(resp); err != nil {
		return nil, err
	}

	return &stats, nil
}
