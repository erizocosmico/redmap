package worker

import (
	"context"
	"fmt"
	"net"

	"github.com/erizocosmico/redmap/internal/proto"
)

// Server is a worker server that handles connections from a master.
type Server struct {
	addr    string
	maxSize uint64
	jobs    *jobRegistry
}

// NewServer creates a new worker server. MaxSize controls the maximum size
// allowed in request data.
func NewServer(
	addr string,
	maxSize uint64,
) *Server {
	return &Server{
		addr:    addr,
		maxSize: maxSize,
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
		j, ok := s.jobs.get(r.ID)
		if !ok {
			return nil, fmt.Errorf("unable to find job %s", r.ID)
		}

		return j.Map(r.Data)
	case proto.HealthCheck:
		return nil, nil
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
		// TODO: handle error
	}
}
