package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/erizocosmico/redmap/internal/manager"
	uuid "github.com/satori/go.uuid"
	cli "gopkg.in/src-d/go-cli.v0"
)

type runCmd struct {
	cli.PlainCommand `name:"run" short-description:"run jobs" long-description:""`
	Manager          string        `long:"manager" short:"m" description:"manager server address"`
	Name             string        `long:"name" short:"n" description:"job name"`
	GoCmd            string        `long:"go" short:"g" default:"go" description:"path to the Go binary"`
	WriteTimeout     time.Duration `long:"write-timeout" default:"10s" description:"maximum time to wait for write operations before aborting"`
	ReadTimeout      time.Duration `long:"read-timeout" default:"10s" description:"maximum time to wait for read operations before aborting"`
}

func (c *runCmd) ExecuteContext(ctx context.Context, args []string) error {
	if c.Manager == "" {
		return fmt.Errorf("manager server address was not provided")
	}

	if len(args) != 1 {
		return fmt.Errorf("expected plugin path, got %q", strings.Join(args, " "))
	}

	path := args[0]

	cli, err := manager.NewClient(c.Manager, &manager.ClientOptions{
		WriteTimeout: c.WriteTimeout,
		ReadTimeout:  c.ReadTimeout,
	})
	if err != nil {
		return err
	}

	defer cli.Close()

	cmdPath, err := exec.LookPath(c.GoCmd)
	if err != nil {
		return fmt.Errorf("cannot find go command at %q: %s", c.GoCmd, err)
	}

	f, err := ioutil.TempFile(os.TempDir(), "redmap-")
	if err != nil {
		return fmt.Errorf("unable to obtain a temporary file: %s", err)
	}

	dst := f.Name()
	_ = f.Close()

	fmt.Println("Compiling plugin...")

	cmd := exec.CommandContext(ctx, cmdPath, "build", "-buildmode=plugin", "-o", dst, path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Unable to compile plugin at", path)
		fmt.Println(string(output))
		return err
	}

	fmt.Println("Plugin compiled successfully.")

	plugin, err := ioutil.ReadFile(dst)
	if err != nil {
		return fmt.Errorf("unable to read compiled plugin at %q: %s", dst, err)
	}

	_ = os.Remove(dst)

	id := uuid.NewV4()
	var name = id.String()
	if c.Name != "" {
		name = c.Name
	}

	if err := cli.RunJob(name, id, plugin); err != nil {
		return err
	}

	fmt.Println("Job started successfully.")
	return nil
}

func init() {
	app.AddCommand(new(runCmd))
}
