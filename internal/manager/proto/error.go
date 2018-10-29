package proto

import (
	wproto "github.com/erizocosmico/redmap/internal/worker/proto"
)

// Err is an error that keeps an underlying error inside.
type Err = wproto.Err

// NewErr creates a new error with an underlying error.
var NewErr = wproto.NewErr

// IsEOF returns whether an error is EOF or not.
var IsEOF = wproto.IsEOF
