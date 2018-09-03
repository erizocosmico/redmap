package worker

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/erizocosmico/redmap/internal/proto"
	"github.com/sirupsen/logrus"
)

const (
	kb             = int32(1024)
	mb             = 1024 * kb
	defaultMaxSize = 200 * mb
)

// Server is a worker server that handles connections from a master.
type Server struct {
	addr       string
	version    string
	maxSize    int32
	activeJobs int32
	jobs       *jobRegistry
}

// ServerOptions provides configuration options for the worker server.
type ServerOptions struct {
	// MaxSize is the maximum allowed size for job data. By default, the max
	// size is 200MB.
	MaxSize int32
	// Version of the server.
	Version string
}

// NewServer creates a new worker server. MaxSize controls the maximum size
// allowed in request data.
func NewServer(
	addr string,
	opts *ServerOptions,
) *Server {
	maxSize := defaultMaxSize
	version := "unknown"
	if opts != nil {
		if opts.MaxSize > 0 {
			maxSize = opts.MaxSize
		}

		if opts.Version != "" {
			version = opts.Version
		}
	}
	return &Server{
		addr:    addr,
		maxSize: maxSize,
		version: version,
		jobs:    newJobRegistry(),
	}
}

// Start listenning to connections.
func (s *Server) Start(ctx context.Context) error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	defer l.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(ctx, conn)
	}
}

func (s *Server) incr() {
	atomic.AddInt32(&s.activeJobs, 1)
}

func (s *Server) decr() {
	atomic.AddInt32(&s.activeJobs, -1)
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	req, err := proto.ParseRequest(conn, s.maxSize)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	resp, err := s.handleRequest(ctx, conn, req)
	if err != nil {
		s.writeError(conn, err)
		return
	}

	s.writeResponse(conn, &proto.Response{Type: proto.Ok, Data: resp})
}

func (s *Server) info() Info {
	return Info{
		Version:       s.version,
		Addr:          s.addr,
		Proto:         proto.Version,
		ActiveJobs:    uint32(atomic.LoadInt32(&s.activeJobs)),
		InstalledJobs: uint32(s.jobs.count()),
	}
}

func (s *Server) handleRequest(
	ctx context.Context,
	conn net.Conn,
	r *proto.Request,
) ([]byte, error) {
	switch r.Op {
	case proto.Install:
		if err := s.jobs.install(r.ID, r.Data); err != nil {
			return nil, err
		}

		return nil, nil
	case proto.Uninstall:
		if err := s.jobs.uninstall(r.ID); err != nil {
			return nil, err
		}

		return nil, nil
	case proto.ExecMap:
		s.incr()
		defer s.decr()

		j, ok := s.jobs.get(r.ID)
		if !ok {
			return nil, fmt.Errorf("unable to find job %s", r.ID)
		}

		return j.Map(r.Data)
	case proto.HealthCheck:
		return nil, nil
	case proto.Info:
		return s.info().Encode()
	default:
		return nil, ErrInvalidJob
	}
}

func (s *Server) writeError(conn net.Conn, err error) {
	s.writeResponse(conn, &proto.Response{
		Type: proto.Error,
		Data: []byte(err.Error()),
	})
}

func (s *Server) writeResponse(conn net.Conn, r *proto.Response) {
	if err := proto.WriteResponse(r, conn); err != nil {
		logrus.WithField("err", err).Error("unable to write response")
	}
}
