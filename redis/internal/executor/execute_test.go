package executor_test

import (
	"testing"

	"github.com/sai29/one2n_go_bootcamp/redis/internal/executor"
)

func TestExecute(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		input string
		want  string
	}{
		{
			name:  "SET command",
			input: "SET name foo",
			want:  "OK",
		},
		{
			name:  "GET command",
			input: "GET name",
			want:  "foo",
		},
		{
			name:  "DEL command",
			input: "DEL name",
			want:  "(integer) 1",
		},
		{
			name:  "DEL command with missing key",
			input: "DEL name",
			want:  "(integer) 0",
		},
		{
			name:  "GET command",
			input: "GET name",
			want:  "(nil)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := executor.Execute(tt.input)

			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
