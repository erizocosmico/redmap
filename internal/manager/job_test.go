package manager

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/erizocosmico/redmap/internal/manager/proto"
	"github.com/erizocosmico/redmap/internal/worker/workertest"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestJobManager(t *testing.T) {
	require := require.New(t)
	plugin, cleanup := compilePlugin(t)
	defer cleanup()

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
			return data, nil
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
	require.Equal("7", string(content))

	_ = os.Remove("result")
}

func compilePlugin(t *testing.T) ([]byte, func()) {
	t.Helper()
	require := require.New(t)

	path := filepath.Join("..", "..", "_testdata", "job.go")
	f, err := ioutil.TempFile(os.TempDir(), "redmap-")
	require.NoError(err)

	dst := f.Name()
	require.NoError(f.Close())

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", dst, path)
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
