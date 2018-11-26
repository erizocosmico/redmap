package proto

import (
	"bytes"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestRequestRoundtrip(t *testing.T) {
	testCases := []struct {
		name       string
		req        *Request
		writeError bool
		parseError bool
	}{
		{"hello", &Request{Op: Hello}, false, false},
		{"stats", &Request{Op: Stats}, false, false},
		{"jobs", &Request{Op: Jobs}, false, false},
		{"jobstats", &Request{
			Op:   JobStats,
			Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8},
		}, false, false},
		{"runjob", &Request{Op: RunJob, Data: []byte{1, 2, 3, 4}}, false, false},
		{"attach", &Request{Op: Attach, Data: []byte{1, 2, 3, 4}}, false, false},
		{"detach", &Request{Op: Detach, Data: []byte{1, 2, 3, 4}}, false, false},
		{"invalid", &Request{Op: Invalid}, true, false},
		{"too large", &Request{Op: RunJob, Data: []byte{1, 2, 3, 4, 5}}, false, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			err := WriteRequest(tt.req, &buf)
			if tt.writeError {
				require.Error(err)
				return
			}
			require.NoError(err)

			req, err := ParseRequest(&buf, 4)
			if tt.parseError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.req, req)
			}
		})
	}
}

func TestJobDataRoundtrip(t *testing.T) {
	require := require.New(t)

	d := JobData{
		Name:   "foo",
		ID:     uuid.NewV4(),
		Plugin: []byte{1, 2, 3, 4},
	}

	data, err := d.Encode()
	require.NoError(err)

	var d2 JobData
	require.NoError(d2.Decode(data))
	require.Equal(d, d2)
}

func TestWorkerDataRoundtrip(t *testing.T) {
	require := require.New(t)

	d := WorkerData{
		Addr: "0.0.0.0:9876",
		Auth: []byte{1, 2, 3, 4},
	}

	data, err := d.Encode()
	require.NoError(err)

	var d2 WorkerData
	require.NoError(d2.Decode(data))
	require.Equal(d, d2)
}
