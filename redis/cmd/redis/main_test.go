package main

import (
	"io"
	"testing"
)

func Test_executeCommand(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		input io.Reader
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executeCommand(tt.input)
		})
	}
}
