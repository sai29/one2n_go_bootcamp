package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/sai29/one2n_go_bootcamp/redis/internal/executor"
)

func main() {
	executeCommand(os.Stdin)
}

func executeCommand(input io.Reader) {
	scanner := bufio.NewScanner(input)
	store := executor.NewStore()
	for {
		fmt.Print("$ ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		command := executor.CreateCommand(line)

		output := store.Execute(command)
		for _, val := range output {
			fmt.Println(">", val)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}
