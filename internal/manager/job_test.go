package manager

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/erizocosmico/redmap/internal/manager/proto"
	"github.com/erizocosmico/redmap/internal/worker/workertest"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestJobManager(t *testing.T) {
	require := require.New(t)
	plugin, cleanup := compilePlugin(t, "job.go")
	defer cleanup()
	defer func() {
		_ = os.Remove("result")
	}()

	wp := newWorkerPool()
	w := newWorker("0.0.0.0:9876", nil)
	wp.add(w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j := newJobManager(ctx, wp, 2, false)

	var maps int32
	server := workertest.NewServer(w.addr, workertest.Hooks{
		OnExecMap: func(id uuid.UUID, data []byte) ([]byte, error) {
			atomic.AddInt32(&maps, 1)
			n, err := strconv.Atoi(string(data))
			if err != nil {
				return nil, err
			}
			return []byte(fmt.Sprint(n + 1)), nil
		},
	})

	go server.Start(ctx)

	require.NoError(j.run(&proto.JobData{
		ID:     uuid.NewV4(),
		Name:   "test",
		Plugin: plugin,
	}))

	require.Equal(int32(3), atomic.LoadInt32(&maps))

	content, err := ioutil.ReadFile("result")
	require.NoError(err)
	require.Equal("6", string(content))
}

func TestJobManagerReduceError(t *testing.T) {
	require := require.New(t)
	plugin, cleanup := compilePlugin(t, "reduce_error.go")
	defer cleanup()

	wp := newWorkerPool()
	w := newWorker("0.0.0.0:9877", nil)
	wp.add(w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j := newJobManager(ctx, wp, 2, false)

	var id uuid.UUID
	server := workertest.NewServer(w.addr, workertest.Hooks{
		OnExecMap: func(i uuid.UUID, data []byte) ([]byte, error) {
			id = i
			return data, nil
		},
	})

	go server.Start(ctx)

	require.NoError(j.run(&proto.JobData{
		ID:     uuid.NewV4(),
		Name:   "test",
		Plugin: plugin,
	}))

	require.NotNil(j.jobs[id])
	require.Len(j.jobs[id].errors, 4)
}

func compilePlugin(t *testing.T, plugin string) ([]byte, func()) {
	t.Helper()
	require := require.New(t)

	// Check if plugin is pre-compiled
	path := filepath.Join("..", "..", "_testdata", plugin)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			data, err := ioutil.ReadFile(path)
			require.NoError(err)
			return data, func() {}
		}
		require.NoError(err)
	}

	f, err := ioutil.TempFile(os.TempDir(), "redmap-")
	require.NoError(err)

	dst := f.Name()
	require.NoError(f.Close())

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-i", "-o", dst, path+".go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(string(out))
	}

	data, err := ioutil.ReadFile(dst)
	require.NoError(err)

	return data, func() {
		require.NoError(os.Remove(dst))
	}
}
