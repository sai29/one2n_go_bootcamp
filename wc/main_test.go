package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestWcCommandWithFile(t *testing.T) {
	// Create a temporary file
	content := []byte("Hello\nWorld\nTest\n")
	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "run", "main.go", tmpfile.Name())
	var out bytes.Buffer
	cmd.Stdout = &out

	err = cmd.Run()
	if err != nil {
		t.Fatalf("cmd.Run() failed with %s\n", err)
	}

	expected := "       3       3      17 " + tmpfile.Name() + "\n"
	if out.String() != expected {
		t.Errorf("Expected output %q, but got %q", expected, out.String())
	}
}

func Test_count(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  fileCount
		err   error
	}{
		{
			name:  "Count all words, lines and chars",
			input: []byte("First line is here\n"),
			want: fileCount{
				words: 4,
				lines: 1,
				chars: 19,
			},
		},
		{
			name:  "Count all words and chars only where there is no newline.",
			input: []byte("First line is here"),
			want: fileCount{
				words: 4,
				lines: 0,
				chars: 18,
			},
		},
		{
			name:  "Error should be sent when there are errors",
			input: []byte(""),
			err:   fmt.Errorf("test error"),
			want: fileCount{
				words: 0,
				lines: 0,
				chars: 0,
				error: fmt.Errorf("test error"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runCount(tt.input, tt.err)
			if !compareFileCount(*got, tt.want) {
				t.Errorf("count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func runCount(input []byte, err error) *fileCount {
	line := make(chan []byte)
	lineErrChan := make(chan error)
	var fileStats fileCount

	go func() {
		defer close(line)
		defer close(lineErrChan)

		if err != nil {
			lineErrChan <- err
		} else {
			line <- input
		}

	}()
	return processLine(line, lineErrChan, &fileStats)
}

func compareFileCount(a, b fileCount) bool {
	if a.error != nil || b.error != nil {
		return a.error.Error() == b.error.Error()
	}
	return a.lines == b.lines && a.words == b.words && a.chars == b.chars
}

func Test_generateOutput(t *testing.T) {
	type args struct {
		count fileCount
		flags WcFlags
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Count without any flags",
			args: args{
				count: fileCount{
					words:    7,
					lines:    4,
					chars:    58,
					fileName: "state.txt",
				},
				flags: WcFlags{w: true, l: true, c: true},
			},
			want: "       4       7      58 state.txt\n",
		},
		{
			name: "Count with all flags",
			args: args{
				count: fileCount{
					words:    7,
					lines:    4,
					chars:    58,
					fileName: "state.txt",
				},
				flags: WcFlags{w: true, l: true, c: true},
			},
			want: "       4       7      58 state.txt\n",
		},
		{
			name: "Count with stdin input",
			args: args{
				count: fileCount{
					words:    7,
					lines:    4,
					chars:    58,
					fileName: "-",
				},
				flags: WcFlags{w: true, l: true, c: true},
			},
			want: "       4       7      58\n",
		},
		{
			name: "Count with -w -c flags",
			args: args{
				count: fileCount{
					words:    7,
					lines:    4,
					chars:    58,
					fileName: "state.txt",
				},
				flags: WcFlags{w: true, l: false, c: true},
			},
			want: "       7      58 state.txt\n",
		},
		{
			name: "Count with error values",
			args: args{
				count: fileCount{
					words:    7,
					lines:    4,
					chars:    58,
					fileName: "state.txt",
					error:    fmt.Errorf("test error"),
				},
				flags: WcFlags{w: true, l: false, c: true},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateOutput(&tt.args.count, tt.args.flags)
			if err != tt.args.count.error {
				t.Errorf("generateOutput() error = %v, wantErr %v", err, tt.args.count.error)
			}
			if got != tt.want {
				t.Errorf("generateOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}
