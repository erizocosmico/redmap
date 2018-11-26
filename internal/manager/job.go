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
	async      bool
	tokens     chan struct{}
	completed  uint32
	running    uint32
	failed     uint32
}

func newJobManager(
	ctx context.Context,
	wp *workerPool,
	maxRetries int,
	async bool,
) *jobManager {
	// TODO(erizocosmico): make this configurable
	var tokens = make(chan struct{}, 1)
	tokens <- struct{}{}

	return &jobManager{
		jobs:       make(map[uuid.UUID]*job),
		workers:    wp,
		maxRetries: maxRetries,
		ctx:        ctx,
		async:      async,
		tokens:     tokens,
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
	log := logrus.WithFields(logrus.Fields{
		"id":   data.ID,
		"name": data.Name,
	})
	log.Info("allocating job")
	if err := jm.allocate(data.ID); err != nil {
		return err
	}

	log.Info("installing job plugin")

	j, err := jm.install(data)
	if err != nil {
		log.Error("install was not successful")
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

	// Run the job asynchronously.
	run := func() error {
		jm.incrMetric(&jm.running)
		defer jm.decrMetric(&jm.running)

		// Generate a new token once the job has been fully processed.
		defer func() {
			jm.tokens <- struct{}{}
		}()

		newJobRunner(
			jm.ctx,
			log,
			j,
			jm.workers,
			data.Plugin,
			jm.maxRetries,
		).run()

		if err := j.Done(j.accumulator); err != nil {
			log.WithField("err", err).
				Error("error occurred calling job Done hook")
			jm.incrMetric(&jm.failed)
			return err
		}

		if j.processed > 0 {
			jm.incrMetric(&jm.completed)
		} else {
			jm.incrMetric(&jm.failed)
		}

		return nil
	}

	// Wait until a slot is available so that we can limit the number of jobs
	// running concurrently.
	<-jm.tokens

	if !jm.async {
		return run()
	}

	go run()

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

	// TODO(erizocosmico): ensure plugin has not been loaded already.
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

	var total int32 = -1
	if cnt, ok := j.(redmap.Countable); ok {
		total, err = cnt.Count()
		if err != nil {
			total = -1
			logrus.WithFields(logrus.Fields{
				"job":  data.ID,
				"name": data.Name,
			}).Error("unable to get the total count of tasks for job")
		}
	}

	return &job{
		Job:        j,
		id:         data.ID,
		name:       data.Name,
		state:      jobWaiting,
		cancel:     cancel,
		loader:     tasks,
		loadErrors: errors,
		pluginPath: f.Name(),
		total:      total,
	}, nil
}

func (jm *jobManager) incrMetric(metric *uint32) {
	jm.mut.Lock()
	*metric++
	jm.mut.Unlock()
}

func (jm *jobManager) decrMetric(metric *uint32) {
	jm.mut.Lock()
	*metric--
	jm.mut.Unlock()
}

type jobRunner struct {
	sync.WaitGroup
	ctx        context.Context
	log        *logrus.Entry
	job        *job
	workers    *workerPool
	tasks      chan task
	errors     chan error
	plugin     []byte
	maxRetries int
}

func newJobRunner(
	ctx context.Context,
	log *logrus.Entry,
	job *job,
	workers *workerPool,
	plugin []byte,
	maxRetries int,
) *jobRunner {
	return &jobRunner{
		ctx:        ctx,
		log:        log,
		job:        job,
		workers:    workers,
		tasks:      make(chan task),
		errors:     make(chan error),
		plugin:     plugin,
		maxRetries: maxRetries,
	}
}

func (r *jobRunner) run() {
	ctx, cancel := context.WithCancel(r.ctx)
	var done = make(chan struct{})
	var errorsDone = make(chan struct{})
	var tasksDone = make(chan struct{})
	go r.produce(ctx, done)

	go func() {
		defer func() {
			close(errorsDone)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-r.errors:
				if !ok {
					return
				}

				r.job.failedTask(err)

				if err == ErrNoWorkersAvailable {
					r.log.Error("no workers available, cancelling job")
					cancel()
					return
				}

				r.log.WithField("err", err).Error("failed task")
			}
		}
	}()

	go func() {
		defer func() {
			close(tasksDone)
		}()

		for {
			select {
			case <-ctx.Done():
				r.job.cancel()
				return
			case task, ok := <-r.tasks:
				if !ok {
					// TODO(erizocosmico): set job as finished
					r.job.cancel()
					return
				}

				if task.retries > r.maxRetries {
					r.errors <- task.lastError
				} else {
					go r.executeTask(ctx, task)
				}
			}
		}
	}()

	// Once all tasks are loaded, we wait for them to be completed.
	<-done
	r.Wait()
	cancel()

	close(r.errors)
	close(r.tasks)

	// Make sure we wait for all the tasks and errors to be completed before
	// we exit.
	<-errorsDone
	<-tasksDone
}

type task struct {
	retries   int
	data      []byte
	lastError error
}

func (r *jobRunner) executeTask(ctx context.Context, task task) {
	defer r.Done()

	r.log.Info("executing task")

	r.job.runningTask()
	worker, err := r.pickWorker(r.job.id)
	if err != nil {
		r.errors <- err
		return
	}

	r.log.WithField("worker", worker.addr).Info("worker was picked")

	// We need to wait until the job is installed on the worker.
	// TODO(erizocosmico): handle installation problems so we don't
	// retry installs on a worker that previously errored.
	worker.waitUntilInstalled(r.job.id)

	requiresInstallation := !worker.isRunningJobTasks(r.job.id)
	worker.addJob(r.job.id)

	requeue := func(err error) {
		r.log.WithField("err", err).
			Error("task failed, so it will be requeued")
		r.Add(1)
		task.retries++
		task.lastError = err
		r.tasks <- task
		worker.failed(r.job.id)
		r.job.requeuedTask()
	}

	worker.running(r.job.id)

	if requiresInstallation {
		r.log.Info("worker requires installation, installing")
		err := worker.install(r.job.id, r.plugin)
		if err != nil {
			requeue(err)
			return
		}
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
	r.log.Info("map executed correctly")

	if err := r.job.reduce(result); err != nil {
		r.errors <- err
		return
	}

	r.log.Info("reduce executed correctly")

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

func (r *jobRunner) produce(ctx context.Context, done chan struct{}) {
	defer func() {
		close(done)
		r.job.cancel()
		r.job.loadErrors = nil
		r.job.loader = nil
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-r.job.loadErrors:
			if err != nil && r.errors != nil {
				r.errors <- err
			}
		case data, ok := <-r.job.loader:
			if !ok {
				return
			}
			r.Add(1)
			r.tasks <- task{data: data}
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
