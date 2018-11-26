package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	uuid "github.com/satori/go.uuid"

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

type jobStatsCmd struct {
	cli.PlainCommand `name:"stats" short-description:"stats about a job" long-description:""`
	Manager          string        `long:"manager" short:"m" description:"address of the manager node"`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *jobStatsCmd) ExecuteContext(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected job id, got %q instead", strings.Join(args, " "))
	}

	id, err := uuid.FromString(args[0])
	if err != nil {
		return fmt.Errorf("invalid job id given")
	}

	cli, err := manager.NewClient(c.Manager, &manager.ClientOptions{
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
	if err != nil {
		return err
	}

	stats, err := cli.JobStats(id)
	if err != nil {
		return fmt.Errorf("unable to retrieve job stats: %s", err)
	}

	rows := [][]string{
		{"ID", stats.ID},
		{"NAME", stats.Name},
		{"STATUS", stats.Status},
		{"ERRORS", fmt.Sprint(stats.Errors)},
		{"ACCUMULATOR SIZE", fmt.Sprint(stats.AccumulatorSize)},
		{"RUNNING TASKS", fmt.Sprint(stats.Tasks.Running)},
		{"FAILED TASKS", fmt.Sprint(stats.Tasks.Failed)},
		{"PROCESSED TASKS", fmt.Sprint(stats.Tasks.Processed)},
		{"TOTAL TASKS", fmt.Sprint(stats.Tasks.Total)},
	}

	w := tablewriter.NewWriter(os.Stdout)

	for _, row := range rows {
		w.Append(row)
	}

	w.Render()
	return nil
}

func init() {
	jobs := app.AddCommand(new(jobsCmd))
	jobs.AddCommand(new(listJobsCmd))
	jobs.AddCommand(new(jobStatsCmd))
}
