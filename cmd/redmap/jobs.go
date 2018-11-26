package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/erizocosmico/redmap/internal/manager"
	cli "gopkg.in/src-d/go-cli.v0"
)

type jobsCmd struct {
	cli.PlainCommand `name:"jobs" short-description:"manage redmap jobs" long-description:""`
}

type listJobsCmd struct {
	cli.PlainCommand `name:"list" short-description:"list redmap jobs in a manager node" long-description:""`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *listJobsCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected manager server address, got %q instead", strings.Join(args, " "))
	}

	cli, err := manager.NewClient(args[0], &manager.ClientOptions{
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
	if err != nil {
		return err
	}

	jobs, err := cli.Jobs()
	if err != nil {
		return fmt.Errorf("unable to retrieve jobs: %s", err)
	}

	w := tablewriter.NewWriter(os.Stdout)
	w.SetHeader([]string{"id", "name", "status"})
	for _, j := range jobs {
		w.Append([]string{
			j.ID,
			j.Name,
			string(j.Status),
		})
	}

	w.Render()
	return nil
}

func init() {
	jobs := app.AddCommand(new(jobsCmd))
	jobs.AddCommand(new(listJobsCmd))
}
