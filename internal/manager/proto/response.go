package proto

import (
	wproto "github.com/erizocosmico/redmap/internal/worker/proto"
)

// ErrTooLarge is returned when the data is larger than the amount permitted by
// the configuration values.
var ErrTooLarge = wproto.ErrTooLarge

// Response for a request.
type Response = wproto.Response

// ResponseType is the type of response.
type ResponseType = wproto.ResponseType

const (
	// InvalidResponse is an invalid response.
	InvalidResponse = wproto.InvalidResponse
	// Ok is a successful response.
	Ok = wproto.Ok
	// Error response.
	Error = wproto.Error
)

// ParseResponse parses a response from the given reader. MaxSize controls
// the maximum allowed size of the data inside the response.
var ParseResponse = wproto.ParseResponse

// WriteResponse writes the response to the given writer.
var WriteResponse = wproto.WriteResponse
