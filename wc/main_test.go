package main

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func Test_readFile(t *testing.T) {
	type args struct {
		filePath   string
		flags      WcFlags
		wg         *sync.WaitGroup
		resultChan chan map[string]int
	}
	tests := []struct {
		name string
		args args
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readFile(tt.args.filePath, tt.args.flags, tt.args.wg, tt.args.resultChan)
		})
	}
}

func Test_readLineByLine(t *testing.T) {
	type args struct {
		customReader *CustomReader
		count        map[string]int
		flags        WcFlags
		input        map[string]bool
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "file with all flags",
			args: args{
				customReader: NewCustomReader(strings.NewReader("YOLO\nyolo\n")),
				count:        map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0},
				flags:        WcFlags{w: true, l: true, c: true},
				input:        map[string]bool{"file": true},
			},
			want: map[string]int{"words": 2, "lines": 2, "chars": 10, "errorCode": 0},
		},
		{
			name: "file with all flags but no newlines",
			args: args{
				customReader: NewCustomReader(strings.NewReader("YOLO")),
				count:        map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0},
				flags:        WcFlags{w: true, l: true, c: true},
				input:        map[string]bool{"file": true},
			},
			want: map[string]int{"words": 1, "lines": 0, "chars": 4, "errorCode": 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readLineByLine(tt.args.customReader, tt.args.count, tt.args.flags, tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readLineByLine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_countWLC(t *testing.T) {
	type args struct {
		flags WcFlags
		count map[string]int
		line  string
		input map[string]bool
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "file with all flags",
			args: args{
				flags: WcFlags{w: true, l: true, c: true},
				count: map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0},
				line:  "the quick brown fox ate the lazy dog",
				input: map[string]bool{"file": true},
			},
			want: map[string]int{"words": 8, "lines": 1, "chars": 36, "errorCode": 0},
		},
		{
			name: "file with only words and lines flags",
			args: args{
				flags: WcFlags{w: true, l: true, c: false},
				count: map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0},
				line:  "the quick brown fox ate the lazy dog\n",
				input: map[string]bool{"file": true},
			},
			want: map[string]int{"words": 8, "lines": 1, "chars": 0, "errorCode": 0},
		},
		{
			name: "STDIN with all flags",
			args: args{
				flags: WcFlags{w: true, l: true, c: true},
				count: map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0},
				line:  "the quick brown fox ate the lazy dog only if the dog was quicker if only",
				input: map[string]bool{"file": true, "lastLine": true},
			},
			want: map[string]int{"words": 16, "lines": 0, "chars": 72, "errorCode": 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countWLC(tt.args.flags, tt.args.count, tt.args.line, tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("countWLC() = %v, want %v", got, tt.want)
			}
		})
	}
}
