package bin

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	require := require.New(t)

	buf := bytes.NewBuffer(nil)
	require.NoError(WriteString(buf, "hello"))

	s, err := ReadString(buf)
	require.NoError(err)
	require.Equal("hello", s)
}

func TestUint16(t *testing.T) {
	require := require.New(t)

	buf := bytes.NewBuffer(nil)
	require.NoError(WriteUint16(buf, 42))

	s, err := ReadUint16(buf)
	require.NoError(err)
	require.Equal(uint16(42), s)
}

func TestUint32(t *testing.T) {
	require := require.New(t)

	buf := bytes.NewBuffer(nil)
	require.NoError(WriteUint32(buf, 42))

	s, err := ReadUint32(buf)
	require.NoError(err)
	require.Equal(uint32(42), s)
}
