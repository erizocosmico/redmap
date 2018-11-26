package manager

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/erizocosmico/redmap/internal/manager/proto"
	workerlib "github.com/erizocosmico/redmap/internal/worker"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxSize              uint32 = 200 * 1024 * 1024
	defaultMaxClientConnections        = 4
	defaultReadTimeout                 = 3 * time.Minute
	defaultWriteTimeout                = 30 * time.Second
	defaultMaxRetries                  = 4

	attachWorkerTimeout = 10 * time.Second
)

// Server is a manager server.
type Server struct {
	addr        string
	version     string
	maxSize     uint32
	workerOpts  *WorkerOptions
	forceDetach bool
	maxRetries  int

	conns   sync.WaitGroup
	workers *workerPool
	jobs    *jobManager
}

// ServerOptions contains configuration settings for the manager server.
type ServerOptions struct {
	MaxSize           uint32
	ForceWorkerDetach bool
	WorkerOptions     *WorkerOptions
	// MaxRetries is the maximum number of times to retry a job task before
	// stopping and marking it as failed.
	MaxRetries int
}

// WorkerOptions are the options to configure the worker clients.
type WorkerOptions = workerlib.ClientOptions

// NewServer creates a new manager server.
func NewServer(addr, version string, opts *ServerOptions) *Server {
	var maxSize = defaultMaxSize
	var forceDetach = false
	var maxRetries = defaultMaxRetries
	var workerOptions = &WorkerOptions{
		ReadTimeout:    defaultReadTimeout,
		WriteTimeout:   defaultWriteTimeout,
		MaxConnections: defaultMaxClientConnections,
		MaxSize:        int32(defaultMaxSize),
	}

	if opts != nil {
		if opts.MaxSize > 0 {
			maxSize = opts.MaxSize
		}

		forceDetach = opts.ForceWorkerDetach

		if opts.WorkerOptions != nil {
			workerOptions = opts.WorkerOptions
		}

		if opts.MaxRetries > 0 {
			maxRetries = opts.MaxRetries
		}
	}

	wp := newWorkerPool()

	return &Server{
		addr:        addr,
		version:     version,
		maxSize:     maxSize,
		workerOpts:  workerOptions,
		forceDetach: forceDetach,
		workers:     wp,
		maxRetries:  maxRetries,
	}
}

// Start listenning to connections.
func (s *Server) Start(ctx context.Context) error {
	s.jobs = newJobManager(ctx, s.workers, s.maxRetries, true)

	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	logrus.Infof("listenning for connections at %s", s.addr)

	defer func() {
		logrus.Infof("shutting down server")
		l.Close()
		s.conns.Wait()
	}()

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

		s.conns.Add(1)
		logrus.Debug("received new connection")
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
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

		resp, err := s.handleRequest(ctx, conn, req)
		if err != nil {
			s.writeError(conn, err)
			continue
		}

		s.writeResponse(conn, &proto.Response{Type: proto.Ok, Data: resp})
	}
}

func (s *Server) handleRequest(
	ctx context.Context,
	conn net.Conn,
	req *proto.Request,
) ([]byte, error) {
	switch req.Op {
	case proto.RunJob:
		data, err := req.JobData()
		if err != nil {
			return nil, err
		}

		logrus.WithFields(logrus.Fields{
			"id":   data.ID,
			"name": data.Name,
		}).Info("got new job")

		return nil, s.jobs.run(data)
	case proto.Hello:
		return s.hello()
	case proto.Stats:
		return s.stats()
	case proto.Attach:
		data, err := req.WorkerData()
		if err != nil {
			return nil, err
		}

		// TODO(erizocosmico): add auth to attach
		return nil, s.attachWorker(data.Addr)
	case proto.Detach:
		data, err := req.WorkerData()
		if err != nil {
			return nil, err
		}

		return nil, s.detachWorker(data.Addr)
	case proto.Jobs:
		return s.jobList()
	case proto.JobStats:
		var id uuid.UUID
		copy(id[:], req.Data)
		return s.jobStats(id)
	default:
		return nil, fmt.Errorf("invalid request op: %d", req.Op)
	}
}

func (s *Server) hello() ([]byte, error) {
	return Info{
		Version: s.version,
		Address: s.addr,
		Proto:   proto.Version,
		Workers: s.workers.addresses(),
	}.Encode()
}

func (s *Server) attachWorker(addr string) error {
	w := newWorker(addr, s.workerOpts)
	if err := w.checkAvailability(attachWorkerTimeout); err != nil {
		return fmt.Errorf("unable to connect to worker at %q: %s", addr, err)
	}

	if s.workers.exists(addr) {
		return fmt.Errorf("worker with address %q already attached", addr)
	}

	s.workers.add(w)
	logrus.Infof("worker %s attached", w.addr)
	return nil
}

func (s *Server) detachWorker(addr string) error {
	w, err := s.workers.get(addr)
	if err != nil {
		return err
	}

	if s.forceDetach {
		logrus.Warn("forcing worker detach is activated but not implemented")
	}

	if w.isAwaitingTermination() {
		return fmt.Errorf("worker at %s is already awaiting termination", w.addr)
	}

	w.awaitTermination(func() {
		s.workers.remove(w)
		logrus.Infof("worker %s was detached", w.addr)
	})
	return nil
}

func (s *Server) stats() ([]byte, error) {
	var stats Stats

	var workers = s.workers.all()
	stats.Workers.Total = uint32(len(workers))

	for _, w := range workers {
		switch w.state {
		case workerFailing:
			stats.Workers.Failing++
		case workerTerminated:
			stats.Workers.Terminated++
		default:
			stats.Workers.Active++
		}
	}

	stats.Jobs.Completed = s.jobs.completed
	stats.Jobs.Failed = s.jobs.failed
	stats.Jobs.Running = s.jobs.running

	return stats.Encode()
}

func (s *Server) jobList() ([]byte, error) {
	var jobs Jobs
	s.jobs.mut.RLock()
	for _, j := range s.jobs.jobs {
		j.mut.RLock()
		state := j.state
		j.mut.RUnlock()

		jobs = append(jobs, Job{
			ID:     j.id.String(),
			Name:   j.name,
			Status: state.String(),
		})
	}
	s.jobs.mut.RUnlock()

	sort.Slice(jobs, func(i, j int) bool {
		return strings.Compare(jobs[i].ID, jobs[j].ID) < 0
	})

	return jobs.Encode()
}

func (s *Server) jobStats(id uuid.UUID) ([]byte, error) {
	s.jobs.mut.RLock()
	job := s.jobs.jobs[id]
	s.jobs.mut.RUnlock()

	if job == nil {
		return nil, fmt.Errorf("job not found in server: %s", id)
	}

	job.mut.RLock()
	defer job.mut.RUnlock()

	stats := &JobStats{
		ID:              job.id.String(),
		Name:            job.name,
		Status:          job.state.String(),
		Errors:          uint32(len(job.errors)),
		AccumulatorSize: uint32(len(job.accumulator)),
	}

	stats.Tasks.Processed = uint32(job.processed)
	stats.Tasks.Failed = uint32(job.failed)
	stats.Tasks.Running = uint32(job.running)
	stats.Tasks.Total = uint32(job.total)

	return stats.Encode()
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
