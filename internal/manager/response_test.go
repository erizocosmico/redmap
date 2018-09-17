package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfoRoundtrip(t *testing.T) {
	require := require.New(t)

	info := Info{
		Version: "1.0.0",
		Address: "0.0.0.0:1234",
		Proto:   1,
		Workers: []string{
			"0.0.0.0:1235",
			"0.0.0.0:1236",
			"0.0.0.0:1237",
		},
	}

	data, err := info.Encode()
	require.NoError(err)

	var result Info
	require.NoError(result.Decode(data))

	require.Equal(info, result)
}

func TestStatsRoundtrip(t *testing.T) {
	require := require.New(t)

	var stats Stats
	stats.Jobs.Completed = 1
	stats.Jobs.Failed = 2
	stats.Jobs.Running = 3
	stats.Workers.Total = 4
	stats.Workers.Running = 4
	stats.Workers.Failing = 4

	data, err := stats.Encode()
	require.NoError(err)

	var result Stats
	require.NoError(result.Decode(data))

	require.Equal(stats, result)
}
