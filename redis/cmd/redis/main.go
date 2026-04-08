package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sai29/one2n_go_bootcamp/redis/internal/executor"
)

func main() {
	dbMaster := executor.NewDbMaster()

	ctx, cancel := context.WithCancel(context.Background())
	handleInterrupt(cancel)

	port := "6379" // or get from env
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		os.Exit(1)
	}
	defer ln.Close()

	go func() {
		if err := listenAndServe(ctx, ln, dbMaster); err != nil {
			fmt.Println("server error:", err)
		}
	}()

	fmt.Println("Server listening on", ln.Addr().String())

	<-ctx.Done()
}

func listenAndServe(ctx context.Context, ln net.Listener, dbMaster *executor.DbMaster) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		go handleConn(ctx, conn, dbMaster)
	}
}

func handleConn(ctx context.Context, conn net.Conn, dbMaster *executor.DbMaster) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	session := executor.NewSession()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !scanner.Scan() {
			return
		}

		line := scanner.Text()
		cmd := executor.CreateCommand(line)

		store := dbMaster.Dbs[session.CurrentDbIndex]

		output := session.Execute(cmd, store)

		fmt.Fprintln(conn, output)

		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "Error reading input:", err)
		}
	}
}

func handleInterrupt(cancel context.CancelFunc) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-interrupt
		fmt.Println("Processing input...")
		cancel()
	}()
}
