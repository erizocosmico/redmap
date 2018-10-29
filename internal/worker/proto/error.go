package proto

import (
	"fmt"
	"io"
)

// Err is an error that keeps an underlying error inside.
type Err struct {
	msg        string
	underlying error
}

// NewErr creates a new error with an underlying error.
func NewErr(underlying error, format string, args ...interface{}) *Err {
	if err, ok := underlying.(*Err); ok {
		underlying = err.underlying
	}
	return &Err{fmt.Sprintf(format, args...), underlying}
}

func (e *Err) Error() string {
	return e.msg
}

// IsEOF returns whether an error is EOF or not.
func IsEOF(err error) bool {
	switch err := err.(type) {
	case *Err:
		return err.underlying == io.EOF
	default:
		return err == io.EOF
	}
}
