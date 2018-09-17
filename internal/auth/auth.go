package auth

import (
	"bytes"
	"fmt"

	"github.com/erizocosmico/redmap/internal/bin"
)

type Method uint16

const (
	InvalidMethod Method = iota
	NoneMethod
	PasswordMethod
	lastMethod
)

func None() ([]byte, error) {
	var buf bytes.Buffer
	if err := bin.WriteUint16(&buf, uint16(NoneMethod)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Password(pwd string) ([]byte, error) {
	var buf bytes.Buffer
	if err := bin.WriteUint16(&buf, uint16(NoneMethod)); err != nil {
		return nil, err
	}

	if err := bin.WriteString(&buf, pwd); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type Authenticator interface {
	Auth([]byte) error
}

func Authenticate(data []byte) error {
	r := bytes.NewReader(data)
	m, err := bin.ReadUint16(r)
	if err != nil {
		return err
	}

	method := Method(m)
	if method == InvalidMethod || method >= lastMethod {
		return fmt.Errorf("invalid authentication method: %d", method)
	}

	var a Authenticator
	switch method {
	case None:
		return nil
	case Password:
		a = nil // TODO
	}
}
