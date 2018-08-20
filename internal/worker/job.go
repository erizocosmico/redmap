package worker

import (
	"errors"
	"io/ioutil"
	"os"
	"plugin"
	"sync"

	"github.com/erizocosmico/redmap"
	uuid "github.com/satori/go.uuid"
)

// ErrInvalidJob is returned when the job is not valid.
var ErrInvalidJob = errors.New("job is not valid")

type job struct {
	path string
	job  redmap.Job
}

type jobRegistry struct {
	sync.RWMutex
	registry map[uuid.UUID]job
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{
		registry: make(map[uuid.UUID]job),
	}
}

func (r *jobRegistry) get(id uuid.UUID) (redmap.Job, bool) {
	r.RLock()
	j, ok := r.registry[id]
	r.RUnlock()
	return j.job, ok
}

func (r *jobRegistry) install(id uuid.UUID, data []byte) error {
	if r.isInstalled(id) {
		return nil
	}

	r.Lock()
	defer r.Unlock()

	f, err := ioutil.TempFile(os.TempDir(), "redmap-"+id.String())
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}

	path := f.Name()

	if err := f.Close(); err != nil {
		return err
	}

	plugin, err := plugin.Open(path)
	if err != nil {
		return err
	}

	sym, err := plugin.Lookup(redmap.Symbol)
	if err != nil {
		return err
	}

	j, ok := sym.(redmap.Job)
	if !ok {
		return ErrInvalidJob
	}

	r.registry[id] = job{
		path: path,
		job:  j,
	}

	return nil
}

func (r *jobRegistry) uninstall(id uuid.UUID) error {
	if !r.isInstalled(id) {
		return nil
	}

	r.Lock()
	defer r.Unlock()

	job := r.registry[id]
	delete(r.registry, id)
	return os.Remove(job.path)
}

func (r *jobRegistry) isInstalled(id uuid.UUID) bool {
	r.RLock()
	_, ok := r.registry[id]
	r.RUnlock()
	return ok
}
