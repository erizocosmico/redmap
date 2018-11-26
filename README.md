# redmap

Experimental distributed golang map-reduce jobs using Go plugins.

**Note:** this is very much a wip and just for learning purposes, please use at your own discretion.

### Table of contents

- [Install](#install)
- [Tutorial](#tutorial)
- [Architecture overview](#architecture)
- Commands overview
  - [Run](#run)
  - [Workers](#workers)
  - [Manager](#manager)
  - [Jobs](#jobs)
- [LICENSE](#license)

### Install

```
go get github.com/erizocosmico/redmap/cmd/...
```

This will install the `redmap` binary on your `$GOPATH/bin` directory. Make sure it's in your path!

### Tutorial

This is a very simple overview of how to use redmap and run map reduce jobs. For a more detailed overview of the project, its architecture and its command take a look at the next sections of this document.

To run a map-reduce job, the first thing we need is to write the job itself. Let's say we want to process locally all numbers from 1 to 10, increment them by one and then sum them all together. Remember this is a very silly job which is orders of magnitude slower than just doing it programmatically in Go.

A job is just a [Go plugin](https://golang.org/pkg/plugin/). When redmap loads the plugin, it will look for an identifier named `Job` satisfying the `redmap.Job` interface.

```go
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
)

// Job exported for redmap.
var Job = job{n: 10}

type job struct {
	10 int
}

// Load returns two channels, one with the elements in the job and another
// that will send errors. Execution will not stop if there is an error, so take
// that in mind.
func (j job) Load(context.Context) (<-chan []byte, <-chan error) {
	var jobs = make(chan []byte)
    var errs = make(chan error)

	for i := 1; i <= j.n; i++ {
        // Simplest way to convert the numbers to bytes is just to convert
        // their string representation to bytes. Then we send them through the
        // task channel. Each element we send, will translate to a redmap task.
		jobs <- []byte(fmt.Sprint(i))
	}

    // If we wanted to send an error instead of a task, because something
    // has failed retrieving such task (imagine something that deals with
    // I/O or databases, etc) we would do:
    // errs <- someError

    // It's very important to close the channels once all tasks are sent.
    // Redmap will not consider the job done until they're closed.
	close(jobs)
    close(errs)

	return jobs, errs
}

// Map transforms an element into something else.
func (job) Map(data []byte) ([]byte, error) {
    // We just convert the data, which we know is a string with a number,
    // to a number again.
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, err
	}

    // Then we increment it and convert it back to bytes.
	return []byte(fmt.Sprint(n + 1)), nil
}

// Reduce receives an accumulator and the current element and produces
// another accumulator. Current can also be an accumulator when two
// partial accumulators are being merged.
func (job) Reduce(acc, current []byte) ([]byte, error) {
    // We convert both byte slices to numbers.
	sum, _ := strconv.Atoi(string(acc))
	n, err := strconv.Atoi(string(current))
	if err != nil {
		return nil, err
	}

    // Then we sume them and convert them back to bytes.
	return []byte(fmt.Sprint(sum + n)), nil
}

// Done will be called when all reduces have completed. This is a hook used
// for you to determine where do you want to store the results. The data
// received is the result of the job.
func (job) Done(data []byte) error {
    // We're just going to write the data to a file called result.
    // Note that this is not a very good practice, as this is being
    // written where this call is executed, not the computer from where
    // you ran the job.
	return ioutil.WriteFile("result", data, 0755)
}

// Count is an optional method you job can implement to provide redmap
// with an extimation of how many tasks will this job contain. Since it's
// optional, you can not implement it and redmap will continue to work.
func (j job) Count() (int32, error) {
	return int32(j.n), nil
}
```

We write this file to `~/job.go`

Then we need to start two things: workers and a manager.

First, we start a manager server at port `9899`:

```
redmap manager start :9899`
```

Then, we start two workers at ports `9876` and `9877`:

```
redmap worker start :9876
```
```
redmap worker start :9877
```

Now that we have two workers running, we need to attach them to the manager.

```
redmap manager attach 127.0.0.1:9876 127.0.0.1:9877
```

Once we have everything ready we can launch our job.

```
redmap run --manager 127.0.0.1:9899 --name test-job ~/job.go
```

After it's processed, since we launched this processes locally, we should see a file called `result` with the result of the job in the same folder we were when we started the manager server.

```
cat result
# outputs 65
```

### Architecture

#### Terminology

- **Worker:** server that will receive tasks to perform.
- **Manager:** server that will manage and coordinate workers in order to execute jobs.
- **Job:** a map-reduce job that's sent to the manager to be executed. For example, adding 1 to each number from 0 to 10 and then summing them would be a valid map-reduce job.
- **Task:** a job can be divided in multiple tasks that need to be completed in order for the job to be completed. For example, in the previous example, adding 1 to the number 1 would be a task.
- **Plugin:** redmap jobs are built using Go plugins, by plugin we mean the built plugin that will be loaded using `plugin.Open` function from the Go standard library. This plugin is sent to the manager server and then distributed across workers as needed. That means, size is important, as plugins are moved across the network several times.

Redmap has two different kind of nodes.

#### Worker nodes

Worker nodes are the ones that will be receiving the tasks in every job to transform the data (that is, execute the `map` operations). In a redmap installation, there should be 1 or more workers attached to a manager.

A worker node can have multiple job plugins installed at the same time and execute maps from those jobs concurrently.

#### Manager nodes

Manager nodes coordinate and manage all attached workers in order to perform all tasks on a job. One worker is expected to only be managed by one master, so in a redmap installation there should only be **one** master per set of workers.

`reduce` operations are executed directly on the manager node as results from `map` operations come from workers, so take into account that all data for the reduce should fit in memory in the machine that acts as manager. This may change in the future, though, and instead `reduce` operations may be executed on workers, making coordination and management the manager's only responsibility.

Currently, a manager is limited to execute only one job concurrently. This, however, will be configurable in the future so it can run multiple jobs at the same time.

### Run

You can run jobs using the following command:

```
redmap run --manager=<manager address> --name job_name ./path/to/plugin.go
```

**Flags:**
- `-m`, `--manager` manager server address
- `-n`, `--name` job name, will default to the job ID
- `-g`, `--go` path to the Go binary (default: go)
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

### Workers

These are all commands available for managing worker nodes using the redmap CLI.

#### Start

You can start a worker server with the following command:

```bash
redmap worker start <worker address>
```

**Flags:**
- `--max-size` (default: `0`) max size to allow on job data in a request.

A worker does not have a manager when it's started, workers need to be attached to managers.

There is currently no way to secure workers and/or managers, but that is in the roadmap, so keep both inaccessible from the outside or they could be used by someone else.

#### Status

You can check which worker nodes are reachable using the following command:

```
redmap worker status <address 1> <address 2> <address 3>
```

You can specify as many worker server addresses as you need.

**Flags:**
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

#### Info

You can get information from a list of worker servers with the following command:

```
redmap worker info <address 1> <address 2> <address N>
```

You can specify as many worker server addresses as you need.

**Flags:**
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)
- `--max-size` max size to allow on job data in a request (default: 0)

### Manager

These are all commands available for managing manager nodes using the redmap CLI.

#### Start

You can start a manager server with the following command:

```bash
redmap manager start <manager address>
```

**Flags:**
- `--write-timeout` maximum time to wait for write operations between manager and workers before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations between manager and workers before aborting (default: 10s)
- `--max-size` max size to allow on responses and requests (default: 0)
- `--force-detach` force detach of workers to immediately detach instead of waiting for them to inish
- `--max-connections` maximum number of connections to keep with each worker (default: 4)
- `--max-retries` maximum number of times to retry a job before considering it failed (default: 4)

A manager does not have any workers available when it's started, workers need to be attached to managers.

There is currently no way to secure workers and/or managers, but that is in the roadmap, so keep both inaccessible from the outside or they could be used by someone else.

#### Attach

You can attach a set of workers to a manager using the following command:

```
redmap manager attach <address 1> <address 2> <address 3>
```

You can specify as many worker server addresses as you need.

**Flags:**
- `-m`, `--manager` address of the manager node
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

#### Detach

You can detach a set of workers from a manager using the following command:

```
redmap manager detach <address 1> <address 2> <address 3>
```

You can specify as many worker server addresses as you need.

**Flags:**
- `-m`, `--manager` address of the manager node
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

#### Stats

You can get statistics from a manager node using the following command:

```
redmap manager stats <manager address>
```

You can specify as many worker server addresses as you need.

**Flags:**
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

### Jobs

These are all commands available for managing jobs using the redmap CLI.

#### List

You can list all jobs in a manager node with the following command:

```bash
redmap jobs list <manager address>
```

**Flags:**
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

#### Stats

You can get stats about a job with the following command:

```bash
redmap jobs stats -m <manager address> <job id>
```

**Flags:**
- `-m`, `--manager` address of the manager node
- `--write-timeout` maximum time to wait for write operations before aborting (default: 10s)
- `--read-timeout` maximum time to wait for read operations before aborting (default: 10s)

### Roadmap

- [x] Worker proto
- [x] Worker client
- [x] Worker server
- [x] Manager proto
- [x] Manager client
- [x] Manager server
- [x] Worker CLI
- [x] Master CLI
- [x] Send jobs via CLI
- [ ] Handle node/job failures
- [x] Compile plugins before sending them as jobs
- [ ] Auth for attaching nodes
- [x] Reuse connections on worker clients
- [ ] Reporting via CLI/web monitor
- [x] Implement retry policies

## License

MIT, see [LICENSE](/LICENSE).