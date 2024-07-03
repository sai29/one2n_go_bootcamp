package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type WcFlags struct {
	w bool
	l bool
	c bool
}

func main() {
	os.Exit(run())
}

func NewWcFlags() WcFlags {
	Wcflags := WcFlags{w: false, l: false, c: false}

	if isFlagPassed("l") {
		Wcflags.l = true
	}
	if isFlagPassed("w") {
		Wcflags.w = true
	}

	if isFlagPassed("c") {
		Wcflags.c = true
	}

	if !isFlagPassed("l") && !isFlagPassed("w") && !isFlagPassed("c") {
		Wcflags.l = true
		Wcflags.c = true
		Wcflags.w = true
	}
	return Wcflags
}

func ParseFlags() {
	flag.Bool("l", false, "for lines")
	flag.Bool("w", false, "for words")
	flag.Bool("c", false, "for chars")
	flag.Parse()
}

func readFile(filePath string, flags WcFlags) int {
	count := map[string]int{"words": 0, "lines": 0, "chars": 0}
	file, err := os.Open(filePath)
	errorCode := 0

	if err != nil {
		fmt.Println(err)
		return 126
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

		line, err := reader.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				errorCode = 0
			} else {
				fmt.Println(err)
				errorCode = 126
			}
			break
		}

		if flags.c {
			count["chars"] += len(line)
		}

		if flags.w {
			words := strings.Fields(line)
			count["words"] += len(words)
		}

		if flags.l {
			count["lines"] += 1
		}
	}
	fmt.Println("Count is:", count)
	return errorCode
}

func run() int {
	ParseFlags()
	filePath := flag.Args()[0]
	flags := NewWcFlags()

	errorCode := readFile(filePath, flags)

	// if isFlagPassed("l") && isFlagPassed("w") && isFlagPassed("c") {
	// 	fmt.Printf("%8v %8v %8v %8v\n", lineCount["lines"], wordCount["words"], wordCount["chars"], filePath)
	// }

	// if isFlagPassed("l") && isFlagPassed("w") {
	// 	fmt.Printf("%8v %8v %8v\n", lineCount["lines"], wordCount["words"], filePath)
	// } else if isFlagPassed("l") {
	// 	fmt.Printf("%8v %8v\n", lineCount["lines"], filePath)
	// } else if isFlagPassed("w") {
	// 	fmt.Printf("%8v %8v\n", wordCount["words"], filePath)
	// } else if isFlagPassed("c") {
	// 	fmt.Printf("%8v %8v\n", charCount["chars"], filePath)
	// }
	return errorCode
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
