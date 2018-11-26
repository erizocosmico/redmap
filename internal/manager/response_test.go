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
	stats.Workers.Active = 5
	stats.Workers.Terminated = 6
	stats.Workers.Failing = 7

	data, err := stats.Encode()
	require.NoError(err)

	var result Stats
	require.NoError(result.Decode(data))

	require.Equal(stats, result)
}

func TestJobsRoundtrip(t *testing.T) {
	require := require.New(t)

	jobs := Jobs{
		Job{
			ID:     "1",
			Name:   "name1",
			Status: "done",
		},
		Job{
			ID:     "2",
			Name:   "name2",
			Status: "waiting",
		},
		Job{
			ID:     "3",
			Name:   "name3",
			Status: "running",
		},
	}

	data, err := jobs.Encode()
	require.NoError(err)

	var result Jobs
	require.NoError(result.Decode(data))

	require.Equal(jobs, result)
}

func TestJobStatsRoundtrip(t *testing.T) {
	require := require.New(t)

	stats := &JobStats{
		ID:              "id",
		Name:            "name",
		Errors:          1,
		AccumulatorSize: 2,
		Status:          "foo",
	}

	stats.Tasks.Failed = 3
	stats.Tasks.Processed = 4
	stats.Tasks.Total = 5
	stats.Tasks.Running = 6

	data, err := stats.Encode()
	require.NoError(err)

	var result JobStats
	require.NoError(result.Decode(data))

	require.Equal(stats, &result)
}
