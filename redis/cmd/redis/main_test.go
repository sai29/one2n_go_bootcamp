package main

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/sai29/one2n_go_bootcamp/redis/internal/executor"
)

func Test_listenAndServe(t *testing.T) {
	tests := []struct {
		name  string
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
	}

	dbMaster := executor.NewDbMaster()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	go func() {
		listenAndServe(ctx, ln, dbMaster)
	}()

	addr := ln.Addr().String()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
			defer conn.Close()

			_, err = conn.Write([]byte(tt.input + "\n"))
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}

			reader := bufio.NewReader(conn)
			got, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("failed to read response: %v", err)
			}

			got = strings.TrimSpace(got)
			if got != tt.want {
				t.Errorf("got '%s', want '%s'", got, tt.want)
			}
		})
	}
}
