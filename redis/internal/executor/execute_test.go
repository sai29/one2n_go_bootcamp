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
			name:  "SET command - second",
			input: "SET second bar",
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
		{
			name:  "INCR command",
			input: "INCR counter",
			want:  "(integer) 1",
		},
		{
			name:  "INCR command with extra arguments",
			input: "INCR counter 1",
			want:  "(error) ERR wrong number of arguments for 'incr' command",
		},
		{
			name:  "INCR command for key with string value",
			input: "INCR second",
			want:  "(error) ERR value is not an integer or out of range",
		},
		{
			name:  "INCR command for non zero value already set",
			input: "INCR counter",
			want:  "(integer) 2",
		},
		{
			name:  "INCRBY command",
			input: "INCRBY counter 5",
			want:  "(integer) 7",
		},
		{
			name:  "INCRBY command without increment count",
			input: "INCRBY counter",
			want:  "(error) ERR wrong number of arguments for 'incrby' command",
		},
		{
			name:  "INCRBY command with string arg",
			input: "INCRBY counter hello",
			want:  "(error) ERR value is not an integer or out of range",
		},
		{
			name:  "INCRBY command for pair not created",
			input: "INCRBY counter1 10",
			want:  "(integer) 10",
		},
	}

	store := executor.NewStore()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.Execute(tt.input)
			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
