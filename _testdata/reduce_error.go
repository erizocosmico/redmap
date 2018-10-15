package main

import (
	"context"
	"fmt"
)

type jobError struct {
	n int
}

// Load returns two channels, one with the elements in the job and another
// that will send errors. Execution will not stop if there is an error.
func (j jobError) Load(context.Context) (<-chan []byte, <-chan error) {
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
func (jobError) Map(data []byte) ([]byte, error) {
	return data, nil
}

// Reduce receives an accumulator and the current element and produces
// another accumulator. Current can also be an accumulator when two
// partial accumulators are being merged.
func (jobError) Reduce(acc, current []byte) ([]byte, error) {
	return nil, fmt.Errorf("some error")
}

func (jobError) Done(data []byte) error {
	return nil
}

func (j jobError) Count() (int32, error) {
	return int32(j.n), nil
}

// Job exported for redmap.
var Job = jobError{n: 3}
