package redmap

import "context"

// Job must be implemented by any jobs sent to redmap. It must be able to
// load elements in the job, map them and reduce them.
// Elements and accumulators are byte slices, so the job is responsible for
// serializing and deserializing all the data.
type Job interface {
	// Load returns two channels, one with the elements in the job and another
	// that will send errors. Execution will not stop if there is an error.
	Load(context.Context) (<-chan []byte, <-chan error)
	// Map transforms an element into something else.
	Map([]byte) ([]byte, error)
	// Reduce receives an accumulator and the current element and produces
	// another accumulator. Current can also be an accumulator when two
	// partial accumulators are being merged.
	Reduce(acc, current []byte) ([]byte, error)
}

// Symbol is the name that must be exported in a redmap job.
const Symbol = "Job"
