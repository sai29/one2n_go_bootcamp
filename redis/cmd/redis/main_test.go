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

func Test_multipleClients(t *testing.T) {
	dbMaster := executor.NewDbMaster()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create linstener: %v", err)
	}
	defer ln.Close()

	go func() {
		listenAndServe(ctx, ln, dbMaster)
	}()

	addr := ln.Addr().String()

	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect c1: %v", err)
	}
	defer c1.Close()
	r1 := bufio.NewReader(c1)

	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect c2: %v", err)
	}
	defer c2.Close()
	r2 := bufio.NewReader(c2)

	c1.Write([]byte("MULTI\n"))
	got1, _ := r1.ReadString('\n')
	if strings.TrimSpace(got1) != "OK" {
		t.Fatalf("c1 MULTI = %q, want OK", got1)
	}

	c1.Write([]byte("SET name John\n"))
	got2, _ := r1.ReadString('\n')
	if strings.TrimSpace(got2) != "QUEUED" {
		t.Fatalf("c1 queued set = %q, want QUEUED", got2)
	}

	c2.Write([]byte("GET name\n"))
	got3, _ := r2.ReadString('\n')
	if strings.TrimSpace(got3) != "(nil)" {
		t.Fatalf("client2 GET before EXEC = %q, want (nil)", got3)
	}

	c1.Write([]byte("EXEC\n"))
	got4, _ := r1.ReadString('\n')
	got4 = strings.TrimSpace(got4)
	if got4 != "1) OK" {
		t.Fatalf("client1 EXEC = %q, want \"1) OK\"", got4)
	}

	c2.Write([]byte("GET name\n"))
	got5, _ := r2.ReadString('\n')
	if strings.TrimSpace(got5) != "John" {
		t.Fatalf("client2 GET after EXEC = %q, want John", got5)
	}

}
