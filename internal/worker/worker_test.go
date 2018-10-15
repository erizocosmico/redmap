package worker

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestWorker(t *testing.T) {
	require := require.New(t)
	addr, stop := newServer(t)
	defer stop()

	cli, cleanup := newClient(t, addr)
	defer cleanup()

	require.NoError(cli.HealthCheck())

	data, cleanup := compilePlugin(t)
	defer cleanup()
	id := uuid.NewV4()

	require.NoError(cli.Install(id, data))

	result, err := cli.ExecMap(id, []byte("foo"))
	require.NoError(err)
	require.Equal([]byte("foo,"), result)

	info, err := cli.Info()
	require.NoError(err)

	require.Equal(&Info{
		Version:       "test",
		Proto:         1,
		Addr:          "0.0.0.0:9876",
		ActiveJobs:    0,
		InstalledJobs: 1,
	}, info)

	require.NoError(cli.Uninstall(id))

	result, err = cli.ExecMap(id, []byte("foo"))
	require.Error(err)
}

func newClient(t *testing.T, addr string) (*Client, func()) {
	t.Helper()
	c, err := NewClient(addr, nil)
	require.NoError(t, err)
	return c, func() {
		require.NoError(t, c.Close())
	}
}

func newServer(t *testing.T) (string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	addr := "0.0.0.0:9876"
	server := NewServer(addr, &ServerOptions{
		Version: "test",
	})

	go func() {
		require.NoError(t, server.Start(ctx))
	}()

	// Server needs some time to start.
	time.Sleep(50 * time.Millisecond)

	return addr, cancel
}

func compilePlugin(t *testing.T) ([]byte, func()) {
	t.Helper()
	require := require.New(t)

	// Check if plugin is pre-compiled
	path := filepath.Join("..", "..", "_testdata", "job")
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
