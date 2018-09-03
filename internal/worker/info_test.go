package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfoRoundtrip(t *testing.T) {
	require := require.New(t)
	info := Info{
		Version:       "foo",
		Addr:          "0.0.0.0:9876",
		Proto:         1,
		ActiveJobs:    2,
		InstalledJobs: 1,
	}

	bytes, err := info.Encode()
	require.NoError(err)

	var i Info
	require.NoError(i.Decode(bytes))

	require.Equal(info, i)
}
