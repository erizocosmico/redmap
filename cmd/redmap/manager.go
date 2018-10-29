package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/erizocosmico/redmap/internal/manager"
	"github.com/sirupsen/logrus"
	cli "gopkg.in/src-d/go-cli.v0"
)

type managerCmd struct {
	cli.PlainCommand `name:"manager" short-description:"manage redmap manager nodes" long-description:""`
}

type startManagerCmd struct {
	cli.PlainCommand `name:"start" short-description:"start redmap manager nodes" long-description:""`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations between manager and workers before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations between manager and workers before aborting"`
	MaxSize          uint32        `long:"max-size" default:"0" description:"max size to allow on responses and requests"`
	ForceDetach      bool          `long:"force-detach" description:"force detach of workers to immediately detach instead of waiting for them to finish"`
	MaxConnections   uint          `long:"max-connections" default:"4" description:"maximum number of connections to keep with each worker"`
	MaxRetries       uint          `long:"max-retries" default:"4" description:"maximum number of times to retry a job before considering it failed"`
}

func (c *startManagerCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected server address, got %q instead", strings.Join(args, " "))
	}

	logrus.Infof("starting manager server at %s", args[0])

	server := manager.NewServer(args[0], version, &manager.ServerOptions{
		MaxSize: c.MaxSize,
		WorkerOptions: &manager.WorkerOptions{
			ReadTimeout:    c.ReadTimeout,
			WriteTimeout:   c.WriteTimeout,
			MaxSize:        int32(c.MaxSize),
			MaxConnections: int(c.MaxConnections),
		},
		ForceWorkerDetach: c.ForceDetach,
		MaxRetries:        int(c.MaxRetries),
	})

	return server.Start(ctx)
}

type attachCmd struct {
	cli.PlainCommand `name:"attach" short-description:"attach worker nodes to a manager" long-description:""`
	Manager          string        `long:"manager" short:"m" description:"address of the manager node"`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *attachCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected server addresses, got nothing instead")
	}

	if c.Manager == "" {
		return fmt.Errorf("manager node address was not provided")
	}

	cli, err := manager.NewClient(c.Manager, &manager.ClientOptions{
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
	if err != nil {
		return err
	}

	defer cli.Close()

	var ok int
	for _, addr := range args {
		if err := cli.Attach(addr); err != nil {
			fmt.Printf("Unable to attach worker %s to manager %s: %s\n", addr, c.Manager, err)
		} else {
			ok++
			fmt.Printf("Worker %s has been attached to %s.\n", addr, c.Manager)
		}
	}

	if ok != len(args) {
		os.Exit(1)
	}

	return nil
}

type detachCmd struct {
	cli.PlainCommand `name:"detach" short-description:"detach worker nodes from a manager" long-description:""`
	Manager          string        `long:"manager" short:"m" description:"address of the manager node"`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *detachCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected server addresses, got nothing instead")
	}

	if c.Manager == "" {
		return fmt.Errorf("manager node address was not provided")
	}

	cli, err := manager.NewClient(c.Manager, &manager.ClientOptions{
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
	if err != nil {
		return err
	}

	defer cli.Close()

	var ok int
	for _, addr := range args {
		if err := cli.Detach(addr); err != nil {
			fmt.Printf("Unable to detach worker %s from manager %s: %s\n", addr, c.Manager, err)
		} else {
			ok++
			fmt.Printf("Worker %s has been detached from %s.\n", addr, c.Manager)
		}
	}

	if ok != len(args) {
		os.Exit(1)
	}

	return nil
}

type managerStatsCmd struct {
	cli.PlainCommand `name:"stats" short-description:"gather statistics from a manager node" long-description:""`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *managerStatsCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected server addresses, got %q", strings.Join(args, " "))
	}

	cli, err := manager.NewClient(args[0], &manager.ClientOptions{
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
	if err != nil {
		return err
	}

	defer cli.Close()

	stats, err := cli.Stats()
	if err != nil {
		return err
	}

	fmt.Printf("Stats for manager node %q\n", args[0])
	w := tablewriter.NewWriter(os.Stdout)

	w.Append([]string{"ADDRESS", args[0]})
	w.Append([]string{"RUNNING WORKERS", fmt.Sprint(stats.Workers.Running)})
	w.Append([]string{"FAILING WORKERS", fmt.Sprint(stats.Workers.Failing)})
	w.Append([]string{"TOTAL WORKERS", fmt.Sprint(stats.Workers.Total)})
	w.Append([]string{"RUNNING JOBS", fmt.Sprint(stats.Jobs.Running)})
	w.Append([]string{"FAILED JOBS", fmt.Sprint(stats.Jobs.Failed)})
	w.Append([]string{"COMPLETED JOBS", fmt.Sprint(stats.Jobs.Completed)})

	w.Render()
	return nil
}

func init() {
	manager := app.AddCommand(new(managerCmd))
	manager.AddCommand(new(startManagerCmd))
	manager.AddCommand(new(attachCmd))
	manager.AddCommand(new(detachCmd))
	manager.AddCommand(new(managerStatsCmd))
}
