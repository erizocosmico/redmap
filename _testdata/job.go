package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
)

type job struct {
	n int
}

// Load returns two channels, one with the elements in the job and another
// that will send errors. Execution will not stop if there is an error.
func (j job) Load(context.Context) (<-chan []byte, <-chan error) {
	var jobs = make(chan []byte, j.n)
	var err = make(chan error, 1)

	for i := 0; i < j.n; i++ {
		jobs <- []byte(fmt.Sprint(i))
	}

	err <- fmt.Errorf("foo")

	close(jobs)

	return jobs, err
}

// Map transforms an element into something else.
func (job) Map(data []byte) ([]byte, error) {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprint(n + 1)), nil
}

// Reduce receives an accumulator and the current element and produces
// another accumulator. Current can also be an accumulator when two
// partial accumulators are being merged.
func (job) Reduce(acc, current []byte) ([]byte, error) {
	sum, err := strconv.Atoi(string(acc))
	if err != nil {
		return nil, err
	}

	n, err := strconv.Atoi(string(current))
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprint(sum + n)), nil
}

func (job) Done(data []byte) error {
	return ioutil.WriteFile("result", data, 0755)
}

func (j job) Count() (int32, error) {
	return int32(j.n), nil
}

// Job exported for redmap.
var Job = job{n: 3}
