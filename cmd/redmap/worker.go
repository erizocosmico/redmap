package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/erizocosmico/redmap/internal/worker"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	cli "gopkg.in/src-d/go-cli.v0"
)

type workerCmd struct {
	cli.PlainCommand `name:"worker" short-description:"manage redmap workers" long-description:""`
}

type startWorkerCmd struct {
	cli.PlainCommand `name:"start" short-description:"start a worker server" long-description:""`
	MaxSize          uint32 `long:"max-size" default:"0" description:"max size to allow on job data in a request."`
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
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
	MaxSize          uint32        `long:"max-size" default:"0" description:"max size to allow on responses."`
}

func (c *workerInfoCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected server addresses, got %q instead", strings.Join(args, " "))
	}

	var serversInfo = make([]*worker.Info, len(args))
	for i, arg := range args {
		cli, err := worker.NewClient(arg, &worker.ClientOptions{
			WriteTimeout:   c.WriteTimeout,
			ReadTimeout:    c.ReadTimeout,
			MaxSize:        int32(c.MaxSize),
			MaxConnections: 1,
		})
		if err != nil {
			return err
		}

		info, err := cli.Info()
		if err != nil {
			return fmt.Errorf("unable to retrieve information from %q: %s", arg, err)
		}

		if err := cli.Close(); err != nil {
			fmt.Println("WARN: unable to close client for server:", arg)
		}

		serversInfo[i] = info
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Address", "Version", "Proto", "Active jobs", "Installed jobs"})

	for _, info := range serversInfo {
		table.Append([]string{
			info.Addr,
			info.Version,
			fmt.Sprint(info.Proto),
			fmt.Sprint(info.ActiveJobs),
			fmt.Sprint(info.InstalledJobs),
		})
	}

	table.Render()
	return nil
}

type workerStatusCmd struct {
	cli.PlainCommand `name:"status" short-description:"check the status of a worker server"`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

type workerStatus struct {
	addr string
	ok   bool
}

func (c *workerStatusCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected server addresses, got %q instead", strings.Join(args, " "))
	}

	var serverStatuses = make([]workerStatus, len(args))
	for i, arg := range args {
		var ok = false
		cli, err := worker.NewClient(arg, &worker.ClientOptions{
			WriteTimeout:   c.WriteTimeout,
			ReadTimeout:    c.ReadTimeout,
			MaxConnections: 1,
		})
		if err == nil {
			if err := cli.HealthCheck(); err == nil {
				ok = true
			}

			if err := cli.Close(); err != nil {
				fmt.Println("WARN: unable to close client for server:", arg)
			}
		}

		serverStatuses[i] = workerStatus{arg, ok}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Address", "Status"})

	for _, s := range serverStatuses {
		var status = "OK"
		if !s.ok {
			status = "UNREACHABLE"
		}

		table.Append([]string{
			s.addr,
			status,
		})
	}

	table.Render()
	return nil
}

func init() {
	workers := app.AddCommand(new(workerCmd))
	workers.AddCommand(new(startWorkerCmd))
	workers.AddCommand(new(workerInfoCmd))
	workers.AddCommand(new(workerStatusCmd))
}
