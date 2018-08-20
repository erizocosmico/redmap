package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseRoundtrip(t *testing.T) {
	testCases := []struct {
		name       string
		resp       *Response
		writeError bool
		parseError bool
	}{
		{"ok", &Response{Type: Ok, Data: []byte{1, 2, 3, 4}}, false, false},
		{"error", &Response{Type: Error, Data: []byte{1, 2, 3, 4}}, false, false},
		{"invalid", &Response{Type: InvalidResponse}, true, false},
		{"too large", &Response{Type: Ok, Data: []byte{1, 2, 3, 4, 5}}, false, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			err := WriteResponse(tt.resp, &buf)
			if tt.writeError {
				require.Error(err)
				return
			}
			require.NoError(err)

			resp, err := ParseResponse(&buf, 4)
			if tt.parseError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.resp, resp)
			}
		})
	}
}
