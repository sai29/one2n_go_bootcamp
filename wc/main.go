package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
)

func main() {
	os.Exit(run())
}

func run() int {
	flag.Bool("l", false, "a bool")
	flag.Parse()
	errorCode := 0

	if isFlagPassed("l") {
		filePath := flag.Args()[0]
		errorCode = readLines(filePath)
	}
	// fmt.Println("error code is\n", errorCode)
	return errorCode
}

func readLines(filePath string) int {
	file, err := os.Open(filePath)
	lineCount := 0

	if err != nil {
		fmt.Println(err)
		return 126
		// log.Fatal("Unable to read input file or permission issue with file"+filePath, err)
	} else {
		info, _ := os.Stat(filePath)
		if info.IsDir() {
			fmt.Printf("wc: %s: Is a directory\n", filePath)
			return 126
		}
	}

	defer file.Close()
	reader := bufio.NewReader(file)

	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		} else {
			lineCount += 1
		}

	}
	// fmt.Println(lineCount, filePath)
	fmt.Printf("%8v %8v\n", lineCount, filePath)
	return 0
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
