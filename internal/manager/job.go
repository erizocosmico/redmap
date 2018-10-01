package manager

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"sort"
	"sync"

	"github.com/erizocosmico/redmap"
	"github.com/erizocosmico/redmap/internal/manager/proto"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// ErrNoWorkersAvailable is returned when there are no more available workers
// to take a job.
var ErrNoWorkersAvailable = errors.New("no workers available")

type jobManager struct {
	ctx        context.Context
	mut        sync.RWMutex
	jobs       map[uuid.UUID]*job
	workers    *workerPool
	maxRetries int
}

func newJobManager(wp *workerPool, maxRetries int) *jobManager {
	return &jobManager{
		jobs:       make(map[uuid.UUID]*job),
		workers:    wp,
		maxRetries: maxRetries,
	}
}

func (jm *jobManager) isRegistered(id uuid.UUID) bool {
	jm.mut.RLock()
	_, ok := jm.jobs[id]
	jm.mut.RUnlock()
	return ok
}

func (jm *jobManager) allocate(id uuid.UUID) error {
	jm.mut.Lock()
	defer jm.mut.Unlock()
	_, ok := jm.jobs[id]
	if ok {
		return fmt.Errorf("job %q already registered", id)
	}

	jm.jobs[id] = nil
	return nil
}

func (jm *jobManager) run(data *proto.JobData) error {
	if err := jm.allocate(data.ID); err != nil {
		return err
	}

	j, err := jm.install(data)
	if err != nil {
		j = &job{
			id:     data.ID,
			name:   data.Name,
			state:  jobErrored,
			errors: []error{err},
		}
	}

	jm.mut.Lock()
	jm.jobs[data.ID] = j
	jm.mut.Unlock()

	if j.state == jobErrored {
		return err
	}

	r := newJobRunner(jm.ctx, j, jm.workers, data.Plugin, jm.maxRetries)
	go r.run()

	return nil
}

func (jm *jobManager) install(data *proto.JobData) (*job, error) {
	f, err := ioutil.TempFile(os.TempDir(), "redmap-job-")
	if err != nil {
		return nil, fmt.Errorf("cannot create temp file to store plugin %q: %s", data.Name, err)
	}

	if _, err := f.Write(data.Plugin); err != nil {
		if err := os.Remove(f.Name()); err != nil {
			logrus.Errorf("unable to remove plugin data at %s: %s", f.Name(), err)
		}
		return nil, fmt.Errorf("unable to install job plugin %q: %s", data.ID, err)
	}

	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("unable to close plugin file %s: %s", f.Name(), err)
	}

	p, err := plugin.Open(f.Name())
	if err != nil {
		return nil, fmt.Errorf("unable to open plugin %s: %s", data.ID, err)
	}

	sym, err := p.Lookup(redmap.Symbol)
	if err != nil {
		return nil, fmt.Errorf("unable to load job symbol from plugin %s: %s", data.ID, err)
	}

	j, ok := sym.(redmap.Job)
	if !ok {
		return nil, fmt.Errorf("job in plugin %s is not a valid job, is: %T", data.ID, sym)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tasks, errors := j.Load(ctx)

	return &job{
		Job:        j,
		id:         data.ID,
		name:       data.Name,
		state:      jobWaiting,
		cancel:     cancel,
		loader:     tasks,
		loadErrors: errors,
		pluginPath: f.Name(),
		total:      -1, // TODO(erizocosmico): add a way to preload total
	}, nil
}

type jobRunner struct {
	sync.WaitGroup
	ctx        context.Context
	job        *job
	workers    *workerPool
	tasks      chan task
	errors     chan error
	plugin     []byte
	maxRetries int
}

func newJobRunner(
	ctx context.Context,
	job *job,
	workers *workerPool,
	plugin []byte,
	maxRetries int,
) *jobRunner {
	return &jobRunner{
		ctx:        ctx,
		job:        job,
		workers:    workers,
		tasks:      make(chan task),
		errors:     make(chan error),
		plugin:     plugin,
		maxRetries: maxRetries,
	}
}

func (r *jobRunner) run() {
	go r.produce()

	ctx, cancel := context.WithCancel(r.ctx)

Loop:
	for {
		select {
		case <-r.ctx.Done():
			r.job.cancel()
			cancel()
			break Loop
		case task, ok := <-r.tasks:
			if !ok {
				// TODO(erizocosmico): set job as finished
				r.job.cancel()
				cancel()
				break Loop
			}

			if task.retries > r.maxRetries {
				r.errors <- task.lastError
			} else {
				go r.executeTask(ctx, task)
			}
		case err, ok := <-r.errors:
			if ok {
				r.job.failedTask(err)

				if err == ErrNoWorkersAvailable {
					r.job.cancel()
					cancel()
					break Loop
				}
			}
		}
	}

	r.Wait()
}

type task struct {
	retries   int
	data      []byte
	lastError error
}

func (r *jobRunner) executeTask(ctx context.Context, task task) {
	defer r.Done()

	r.job.runningTask()
	worker, err := r.pickWorker(r.job.id)
	if err != nil {
		r.errors <- err
		return
	}
	requiresInstallation := worker.isRunningJobTasks(r.job.id)

	requeue := func(err error) {
		r.Add(1)
		task.retries++
		task.lastError = err
		r.tasks <- task
		worker.failed(r.job.id)
		r.job.requeuedTask()
	}

	worker.running(r.job.id)

	if requiresInstallation {
		err = Retry(installTimeout, func() error {
			cli, err := worker.client()
			if err != nil {
				return err
			}

			return cli.Install(r.job.id, r.plugin)
		})
	}

	if err != nil {
		requeue(err)
		return
	}

	var result []byte
	cli, err := worker.client()
	if err != nil {
		requeue(err)
		return
	}

	result, err = cli.ExecMap(r.job.id, task.data)
	if err != nil {
		// TODO(erizocosmico): handle if it's job error or worker error
		requeue(err)
		return
	}

	worker.processed(r.job.id)

	if err := r.job.reduce(result); err != nil {
		r.errors <- err
		return
	}

	r.job.processedTask()
}

func (r *jobRunner) pickWorker(job uuid.UUID) (*worker, error) {
	workers := r.workers.all()
	var loads = make([]uint32, len(workers))
	for i, w := range workers {
		loads[i] = w.load()
	}

	sort.Slice(workers, func(i, j int) bool {
		if loads[i] == loads[j] {
			if workers[i].isRunningJobTasks(job) {
				return true
			}

			if workers[j].isRunningJobTasks(job) {
				return false
			}
		}

		return loads[i] < loads[j]
	})

	for _, w := range workers {
		if w.isAvailable() {
			return w, nil
		}
	}

	return nil, ErrNoWorkersAvailable
}

func (r *jobRunner) produce() {
	cleanup := func() {
		r.job.cancel()
		close(r.tasks)
		close(r.errors)
		r.job.loadErrors = nil
		r.job.loader = nil
	}

	for {
		select {
		case <-r.ctx.Done():
			cleanup()
			return
		case data, ok := <-r.job.loader:
			if !ok {
				cleanup()
				return
			}
			r.Add(1)
			r.tasks <- task{data: data}
		case err := <-r.job.loadErrors:
			r.errors <- err
		}
	}
}

type jobState byte

const (
	jobWaiting jobState = iota
	jobErrored
	jobRunning
	jobFinished
)

type job struct {
	redmap.Job
	id         uuid.UUID
	name       string
	state      jobState
	loader     <-chan []byte
	loadErrors <-chan error
	pluginPath string
	cancel     context.CancelFunc

	mut         sync.RWMutex
	errors      []error
	accumulator []byte
	failed      int32
	processed   int32
	running     int32
	total       int32
}

func (j *job) requeuedTask() {
	j.mut.Lock()
	defer j.mut.Unlock()
	j.running--
}

func (j *job) runningTask() {
	j.mut.Lock()
	defer j.mut.Unlock()
	j.running++
}

func (j *job) processedTask() {
	j.mut.Lock()
	defer j.mut.Unlock()
	j.processed++
	j.running--
}

func (j *job) reduce(step []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// TODO(erizocosmico): requeue reduce jobs
			err = fmt.Errorf("unable to reduce: %v", r)
		}
	}()

	j.mut.Lock()
	defer j.mut.Unlock()

	result, err := j.Reduce(j.accumulator, step)
	if err != nil {
		return err
	}

	j.accumulator = result
	return err
}

func (j *job) failedTask(err error) {
	j.mut.Lock()
	defer j.mut.Unlock()
	j.errors = append(j.errors, err)
	j.failed++
	j.running--
}
