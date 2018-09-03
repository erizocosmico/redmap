package worker

import (
	"context"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestWorker(t *testing.T) {
	require := require.New(t)
	addr, stop := newServer(t)
	defer stop()

	cli := newClient(t, addr)

	require.NoError(cli.HealthCheck())

	data := compilePlugin(t)
	id := uuid.NewV4()

	require.NoError(cli.Install(id, data))

	result, err := cli.ExecMap(id, []byte("foo"))
	require.NoError(err)
	require.Equal([]byte("foo,"), result)

	require.NoError(cli.Uninstall(id))

	result, err = cli.ExecMap(id, []byte("foo"))
	require.Error(err)
}

func newClient(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := NewClient(addr, 0, 0, math.MaxUint64)
	require.NoError(t, err)
	return c
}

func newServer(t *testing.T) (string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	addr := "0.0.0.0:9876"
	server := NewServer(addr, math.MaxUint64)

	go func() {
		require.NoError(t, server.Start(ctx))
	}()

	return addr, cancel
}

func compilePlugin(t *testing.T) []byte {
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

	return data
}
