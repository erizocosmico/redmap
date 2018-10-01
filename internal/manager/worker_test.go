package manager

import (
	"context"
	"testing"
	"time"

	"github.com/erizocosmico/redmap/internal/worker/workertest"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	require := require.New(t)

	pool := newWorkerPool()

	w1 := newWorker(":1234", nil)
	w2 := newWorker(":1235", nil)

	pool.add(w1)
	pool.add(w2)

	w, err := pool.get(w1.addr)
	require.NoError(err)
	require.Equal(w1, w)

	_, err = pool.get("foo")
	require.Error(err)

	require.Equal(
		[]*worker{w1, w2},
		pool.all(),
	)

	require.True(pool.exists(w2.addr))
	require.False(pool.exists("foo"))

	require.Equal(
		[]string{w1.addr, w2.addr},
		pool.addresses(),
	)

	pool.remove(w1)

	require.False(pool.exists(w1.addr))
}

func TestWorker(t *testing.T) {
	require := require.New(t)

	id1 := uuid.NewV4()
	id2 := uuid.NewV4()

	worker := newWorker("0.0.0.0:9123", nil)
	worker.addJob(id1)
	worker.addJob(id2)

	worker.running(id1)
	worker.running(id2)
	worker.running(id2)

	require.Equal(uint32(3), worker.load())
	require.Equal(uint32(3), worker.pendingTasks())
	worker.processed(id2)
	require.Equal(uint32(2), worker.pendingTasks())
	worker.failed(id2)
	require.Equal(uint32(1), worker.pendingTasks())

	require.True(worker.isRunningJobTasks(id1))
	require.False(worker.isRunningJobTasks(uuid.NewV4()))
	require.True(worker.isAvailable())
	require.False(worker.isAwaitingTermination())

	var terminated bool
	worker.awaitTermination(func() {
		terminated = true
	})

	require.True(worker.isAwaitingTermination())
	require.False(worker.isAvailable())

	require.Error(worker.checkAvailability(50 * time.Millisecond))

	var uninstalled = make(map[uuid.UUID]bool)
	var installed = make(map[uuid.UUID][]byte)
	server := workertest.NewServer(worker.addr, workertest.Hooks{
		OnUninstall: func(id uuid.UUID) {
			uninstalled[id] = true
		},
		OnInstall: func(id uuid.UUID, plugin []byte) {
			installed[id] = plugin
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)

	plugin := []byte{1, 2, 3, 4}
	require.NoError(worker.install(id1, plugin))
	require.Equal(
		map[uuid.UUID][]byte{
			id1: plugin,
		},
		installed,
	)
	require.Len(worker.installed, 1)
	worker.close()

	require.NoError(worker.checkAvailability(50 * time.Millisecond))

	worker.processed(id1)
	require.True(terminated)
	require.True(uninstalled[id1])
	require.True(uninstalled[id2])
	require.Len(worker.installed, 0)

	require.Equal(workerTerminated, worker.state)
}
