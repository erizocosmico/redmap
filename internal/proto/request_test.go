package proto

import (
	"bytes"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestRequestRoundtrip(t *testing.T) {
	uuid := uuid.NewV4()
	testCases := []struct {
		name       string
		req        *Request
		writeError bool
		parseError bool
	}{
		{"healthcheck", &Request{Op: HealthCheck}, false, false},
		{"install", &Request{Op: Install, ID: uuid, Data: []byte{1, 2, 3, 4}}, false, false},
		{"exec map", &Request{Op: ExecMap, ID: uuid, Data: []byte{1, 2, 3, 4}}, false, false},
		{"exec map", &Request{Op: Uninstall, ID: uuid}, false, false},
		{"invalid", &Request{Op: Invalid, ID: uuid}, true, false},
		{"too large", &Request{Op: Install, ID: uuid, Data: []byte{1, 2, 3, 4, 5}}, false, true},
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
