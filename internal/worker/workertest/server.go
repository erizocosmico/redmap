package workertest

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/erizocosmico/redmap/internal/worker"
	"github.com/erizocosmico/redmap/internal/worker/proto"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxSize int32 = 200 * 1024 * 1024
)

// Server is a test implementation of a worker server.
type Server struct {
	Hooks
	addr    string
	version string
	maxSize int32
}

// Hooks provides hooks to intercept calls to the server.
type Hooks struct {
	OnInstall     func(uuid.UUID, []byte)
	OnUninstall   func(uuid.UUID)
	OnHealthcheck func()
	OnInfo        func()
	OnExecMap     func(uuid.UUID, []byte) ([]byte, error)
}

// NewServer creates a new test worker server.
func NewServer(
	addr string,
	hooks Hooks,
) *Server {
	return &Server{
		Hooks:   hooks,
		addr:    addr,
		maxSize: defaultMaxSize,
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

		s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	for {
		req, err := proto.ParseRequest(conn, s.maxSize)
		if err != nil {
			if err == io.EOF {
				break
			}

			s.writeError(conn, err)
			return
		}

		resp, err := s.handleRequest(ctx, conn, req)
		if err != nil {
			s.writeError(conn, err)
			continue
		}

		s.writeResponse(conn, &proto.Response{Type: proto.Ok, Data: resp})
	}
}

func (s *Server) info() worker.Info {
	return worker.Info{
		Version:       "test",
		Addr:          s.addr,
		Proto:         proto.Version,
		ActiveJobs:    0,
		InstalledJobs: 0,
	}
}

func (s *Server) handleRequest(
	ctx context.Context,
	conn net.Conn,
	r *proto.Request,
) ([]byte, error) {
	switch r.Op {
	case proto.Install:
		if s.OnInstall != nil {
			s.OnInstall(r.ID, r.Data)
		}

		return nil, nil
	case proto.Uninstall:
		if s.OnUninstall != nil {
			fmt.Println("hninstall")
			s.OnUninstall(r.ID)
		}

		return nil, nil
	case proto.ExecMap:
		if s.OnExecMap != nil {
			return s.OnExecMap(r.ID, r.Data)
		}

		return nil, fmt.Errorf("ExecMap hook not provided")
	case proto.HealthCheck:
		if s.OnHealthcheck != nil {
			s.OnHealthcheck()
		}

		return nil, nil
	case proto.Info:
		if s.OnInfo != nil {
			s.OnInfo()
		}

		return s.info().Encode()
	default:
		return nil, worker.ErrInvalidJob
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
