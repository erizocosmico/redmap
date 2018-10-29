package worker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/erizocosmico/redmap/internal/worker/proto"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxSize int32 = 200 * 1024 * 1024
)

// Server is a worker server that handles connections from a master.
type Server struct {
	addr       string
	version    string
	maxSize    int32
	activeJobs int32
	jobs       *jobRegistry
	conns      sync.WaitGroup
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

	logrus.Infof("listenning for connections at %s", s.addr)

	defer func() {
		logrus.Infof("shutting down server")
		s.conns.Wait()
	}()

	var done = make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-done:
				return nil
			default:
				return err
			}
		}

		log := logrus.WithField("addr", conn.RemoteAddr().String())
		log.Debugf("got a connection")

		s.conns.Add(1)
		go s.handleConn(ctx, conn, log)
	}
}

func (s *Server) incr() {
	atomic.AddInt32(&s.activeJobs, 1)
}

func (s *Server) decr() {
	atomic.AddInt32(&s.activeJobs, -1)
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn, log *logrus.Entry) {
	defer conn.Close()
	defer s.conns.Done()

	for {
		req, err := proto.ParseRequest(conn, s.maxSize)
		if err != nil {
			if proto.IsEOF(err) {
				break
			}

			s.writeError(conn, err)
			return
		}

		log.Debug("got a new request")

		resp, err := s.handleRequest(ctx, conn, req)
		if err != nil {
			log.Errorf("request failed: %s", err)
			s.writeError(conn, err)
			continue
		}

		logrus.Debug("request was successful")

		s.writeResponse(conn, &proto.Response{Type: proto.Ok, Data: resp})
	}
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

		logrus.WithField("id", r.ID).Info("plugin for job was installed")

		return nil, nil
	case proto.Uninstall:
		if err := s.jobs.uninstall(r.ID); err != nil {
			return nil, err
		}

		logrus.WithField("id", r.ID).Info("plugin for job was uninstalled")

		return nil, nil
	case proto.ExecMap:
		s.incr()
		defer s.decr()

		logrus.WithField("id", r.ID).Info("executing map for job")

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
