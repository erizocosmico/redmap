package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/erizocosmico/redmap/internal/worker"
	"github.com/sirupsen/logrus"
	cli "gopkg.in/src-d/go-cli.v0"
)

type workerCmd struct {
	cli.PlainCommand `name:"worker" short-description:"manage redmap workers" long-description:""`
}

type startWorkerCmd struct {
	cli.PlainCommand `name:"start" short-description:"start a worker server" long-description:""`
	MaxSize          uint32 `name:"max-size" default:"0" description:"max size to allow on job data in a request."`
}

func (c *startWorkerCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected server address, got %q instead", strings.Join(args, " "))
	}

	logrus.Infof("starting worker server at %s", args[0])

	server := worker.NewServer(args[0], &worker.ServerOptions{
		MaxSize: int32(c.MaxSize),
		Version: version,
	})

	return server.Start(ctx)
}

type workerInfoCmd struct {
	cli.PlainCommand `name:"info" short-description:"get information about a worker server"`
}

type workerStatusCmd struct {
	cli.PlainCommand `name:"status" short-description:"check the status of a worker server"`
}

func init() {
	workers := app.AddCommand(new(workerCmd))
	workers.AddCommand(new(startWorkerCmd))
	workers.AddCommand(new(workerInfoCmd))
	workers.AddCommand(new(workerStatusCmd))
}
