package worker

import (
	"fmt"
	"net"
	"time"

	"github.com/erizocosmico/redmap/internal/proto"
	uuid "github.com/satori/go.uuid"
)

// Client executes operations on a worker.
type Client struct {
	addr         string
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxSize      uint64
}

// NewClient creates a new client to operate a specific worker.
// WriteTimeout and readTimeout control the time before reads and writes to
// the worker will timeout. MaxSize controls the maximum allowed size in
// response data.
func NewClient(
	addr string,
	writeTimeout, readTimeout time.Duration,
	maxSize uint64,
) (*Client, error) {
	return &Client{addr, readTimeout, writeTimeout, maxSize}, nil
}

func (c *Client) conn() (net.Conn, error) {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return nil, fmt.Errorf("can't create worker client: %s", err)
	}

	if c.readTimeout > 0 {
		err := conn.SetReadDeadline(time.Now().Add(c.writeTimeout + c.readTimeout))
		if err != nil {
			return nil, err
		}
	}

	if c.writeTimeout > 0 {
		err := conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if err != nil {
			return nil, err
		}
	}

	return conn, nil
}

// HealthCheck returns an error if the worker cannot be reached.
func (c *Client) HealthCheck() error {
	_, err := c.request(&proto.Request{Op: proto.HealthCheck})
	return err
}

// Install installs the plugin for a job on the worker.
func (c *Client) Install(id uuid.UUID, plugin []byte) error {
	_, err := c.request(&proto.Request{
		Op:   proto.Install,
		ID:   id,
		Data: plugin,
	})
	return err
}

// ExecMap executes a map operation on the worker.
func (c *Client) ExecMap(id uuid.UUID, data []byte) ([]byte, error) {
	return c.request(&proto.Request{
		Op:   proto.ExecMap,
		ID:   id,
		Data: data,
	})
}

// Uninstall removes the plugin of the job with the given id from the worker.
func (c *Client) Uninstall(id uuid.UUID) error {
	_, err := c.request(&proto.Request{
		Op: proto.Uninstall,
		ID: id,
	})
	return err
}

func (c *Client) request(r *proto.Request) ([]byte, error) {
	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	err = proto.WriteRequest(r, conn)
	if err != nil {
		return nil, err
	}

	resp, err := proto.ParseResponse(conn, c.maxSize)
	if err != nil {
		return nil, err
	}

	switch resp.Type {
	case proto.Ok:
		return resp.Data, nil
	case proto.Error:
		return nil, fmt.Errorf(string(resp.Data))
	default:
		return nil, fmt.Errorf("got unknown response from worker: %s", c.addr)
	}
}
