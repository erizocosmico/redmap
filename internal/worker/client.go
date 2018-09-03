package worker

import (
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/erizocosmico/redmap/internal/worker/proto"
	"github.com/fatih/pool"
	uuid "github.com/satori/go.uuid"
)

var defaultMaxConnections = runtime.NumCPU()

// Client executes operations on a worker.
type Client struct {
	addr         string
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxSize      int32
	pool         pool.Pool
}

// ClientOptions provides configuration options for the worker client.
type ClientOptions struct {
	// WriteTimeout is the maximum time to wait for a write operation before
	// it is cancelled.
	WriteTimeout time.Duration
	// ReadTimeout is the maximum time to wait for a read operation before
	// it is cancelled.
	ReadTimeout time.Duration
	// MaxSize is the maximum amount of bytes allowed in responses from the
	// server. By default, it's 200MB.
	MaxSize int32
	// MaxConnections is the maximum number of simultaneous connections with
	// the worker to keep.
	MaxConnections int
}

// NewClient creates a new client to operate a specific worker.
// WriteTimeout and readTimeout control the time before reads and writes to
// the worker will timeout. MaxSize controls the maximum allowed size in
// response data.
func NewClient(
	addr string,
	opts *ClientOptions,
) (*Client, error) {
	var readTimeout, writeTimeout time.Duration
	var maxSize = defaultMaxSize
	var maxConnections = defaultMaxConnections
	if opts != nil {
		readTimeout = opts.ReadTimeout
		writeTimeout = opts.WriteTimeout
		if opts.MaxSize > 0 {
			maxSize = opts.MaxSize
		}

		if opts.MaxConnections > 0 {
			maxConnections = opts.MaxConnections
		}
	}

	pool, err := pool.NewChannelPool(1, maxConnections, func() (net.Conn, error) {
		return net.Dial("tcp", addr)
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		addr,
		readTimeout,
		writeTimeout,
		maxSize,
		pool,
	}, nil
}

// Close all the connections in the client.
func (c *Client) Close() error {
	c.pool.Close()
	return nil
}

func (c *Client) conn() (net.Conn, error) {
	conn, err := c.pool.Get()
	if err != nil {
		return nil, fmt.Errorf("can't establish connection with worker at %s: %s", c.addr, err)
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

// Info retrieves the worker info.
func (c *Client) Info() (*Info, error) {
	var info Info
	data, err := c.request(&proto.Request{Op: proto.Info})
	if err != nil {
		return nil, err
	}

	if err := info.Decode(data); err != nil {
		return nil, err
	}

	return &info, nil
}

func (c *Client) request(r *proto.Request) ([]byte, error) {
	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	err = proto.WriteRequest(r, conn)
	if err != nil {
		return nil, fmt.Errorf("could not write request: %s", err)
	}

	resp, err := proto.ParseResponse(conn, c.maxSize)
	if err != nil {
		return nil, fmt.Errorf("could not parse response: %s", err)
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
