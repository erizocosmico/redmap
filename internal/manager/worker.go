package manager

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cenkalti/backoff"
	workerlib "github.com/erizocosmico/redmap/internal/worker"
	uuid "github.com/satori/go.uuid"
)

type workerPool struct {
	mut     sync.RWMutex
	workers map[string]*worker
}

func newWorkerPool() *workerPool {
	return &workerPool{workers: make(map[string]*worker)}
}

func (p *workerPool) all() []*worker {
	p.mut.RLock()
	defer p.mut.RUnlock()

	var result = make([]*worker, 0, len(p.workers))
	for _, w := range p.workers {
		result = append(result, w)
	}
	return result
}

func (p *workerPool) get(addr string) (*worker, error) {
	p.mut.RLock()
	defer p.mut.RUnlock()

	w, ok := p.workers[addr]
	if !ok {
		return nil, fmt.Errorf("can't find worker with address %q", addr)
	}

	return w, nil
}

func (p *workerPool) exists(addr string) bool {
	p.mut.RLock()
	defer p.mut.RUnlock()
	_, ok := p.workers[addr]
	return ok
}

func (p *workerPool) add(w *worker) {
	p.mut.Lock()
	p.workers[w.addr] = w
	p.mut.Unlock()
}

func (p *workerPool) remove(w *worker) {
	p.mut.Lock()
	delete(p.workers, w.addr)
	p.mut.Unlock()
}

func (p *workerPool) addresses() []string {
	p.mut.RLock()
	defer p.mut.RUnlock()

	var addrs = make([]string, 0, len(p.workers))
	for _, w := range p.workers {
		addrs = append(addrs, w.addr)
	}

	sort.Strings(addrs)
	return addrs
}

type workerState byte

const (
	workerOk workerState = iota
	workerAwaitingTermination
	workerFailing
)

type workerJobs struct {
	processed uint32
	failed    uint32
	running   uint32
}

type worker struct {
	cli           *workerlib.Client
	opts          *workerlib.ClientOptions
	addr          string
	state         workerState
	mut           sync.RWMutex
	jobs          map[uuid.UUID]*workerJobs
	onTermination func()
}

func newWorker(addr string, opts *workerlib.ClientOptions) *worker {
	return &worker{
		opts:  opts,
		addr:  addr,
		state: workerOk,
		jobs:  make(map[uuid.UUID]*workerJobs),
	}
}

func (w *worker) load() uint32 {
	w.mut.RLock()
	defer w.mut.RUnlock()

	var load uint32
	for _, j := range w.jobs {
		load += j.running
	}
	return load
}

func (w *worker) isRunningJobTasks(job uuid.UUID) bool {
	w.mut.RLock()
	defer w.mut.RUnlock()
	_, ok := w.jobs[job]
	return ok
}

func (w *worker) isAvailable() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.state == workerOk
}

func (w *worker) isAwaitingTermination() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.state == workerAwaitingTermination
}

func (w *worker) awaitTermination(handler func()) {
	w.mut.Lock()
	w.state = workerAwaitingTermination
	w.onTermination = handler
	w.mut.Unlock()
}

func (w *worker) terminate() {
	w.mut.RLock()

	if w.onTermination != nil {
		w.onTermination()
	}

	w.mut.RUnlock()
}

func (w *worker) checkAvailability() error {
	return backoff.Retry(func() error {
		cli, err := workerlib.NewClient(w.addr, w.opts)
		if err != nil {
			return err
		}

		defer cli.Close()

		return cli.HealthCheck()
	}, backoff.NewExponentialBackOff())
}

func (w *worker) client() (*workerlib.Client, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	if w.cli == nil {
		err := backoff.Retry(func() error {
			var err error
			w.cli, err = workerlib.NewClient(w.addr, w.opts)
			if err != nil {
				return err
			}

			return w.cli.HealthCheck()
		}, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, err
		}
	}

	return w.cli, nil
}

func (w *worker) pendingTasks() uint32 {
	w.mut.RLock()
	defer w.mut.RUnlock()

	var pending uint32
	for _, j := range w.jobs {
		pending += j.running
	}
	return pending
}

func (w *worker) addJob(id uuid.UUID) {
	w.mut.Lock()
	w.jobs[id] = new(workerJobs)
	w.mut.Unlock()
}

func (w *worker) processed(id uuid.UUID) {
	w.mut.Lock()
	if w.jobs[id].running > 0 {
		w.jobs[id].running--
	}

	w.jobs[id].processed++
	w.mut.Unlock()

	if w.isAwaitingTermination() {
		w.terminateIfDone()
	}
}

func (w *worker) failed(id uuid.UUID) {
	w.mut.Lock()
	if w.jobs[id].running > 0 {
		w.jobs[id].running--
	}

	w.jobs[id].failed++
	w.mut.Unlock()

	if w.isAwaitingTermination() {
		w.terminateIfDone()
	}
}

func (w *worker) running(id uuid.UUID) {
	w.mut.Lock()
	w.jobs[id].running++
	w.mut.Unlock()
}

func (w *worker) terminateIfDone() {
	if w.pendingTasks() > 0 {
		// TODO(erizocosmico): free resources (plugins) on termination
		w.terminate()
	}
}
