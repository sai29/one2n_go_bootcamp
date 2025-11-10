package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	fmt.Println("Key value db - early redis DIY")
	executeCommand(os.Stdin)
}

func executeCommand(input io.Reader) {

}
