package main

import (
	"context"
	"fmt"
)

type job struct{}

// Load returns two channels, one with the elements in the job and another
// that will send errors. Execution will not stop if there is an error.
func (job) Load(context.Context) (<-chan []byte, <-chan error) {
	var jobs = make(chan []byte, 3)
	var err = make(chan error, 1)

	for i := 0; i < 3; i++ {
		jobs <- []byte(fmt.Sprint(i))
	}

	err <- fmt.Errorf("foo")

	return jobs, err
}

// Map transforms an element into something else.
func (job) Map(data []byte) ([]byte, error) {
	return append(data, byte(',')), nil
}

// Reduce receives an accumulator and the current element and produces
// another accumulator. Current can also be an accumulator when two
// partial accumulators are being merged.
func (job) Reduce(acc, current []byte) ([]byte, error) {
	return append(acc, current...), nil
}

// Job exported for redmap.
var Job job
