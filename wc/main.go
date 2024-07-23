package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type WcFlags struct {
	w bool
	l bool
	c bool
}

const openFileLimit = 10

type fileCount struct {
	flags               WcFlags
	fileName            string
	words, chars, lines int
	errorCode           int
}

func main() {
	os.Exit(run())
}

func newWcFlags() WcFlags {
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

func parseFlags() {
	flag.Bool("l", false, "for lines")
	flag.Bool("w", false, "for words")
	flag.Bool("c", false, "for chars")
	flag.Parse()
}

func run() int {

	parseFlags()
	flags := newWcFlags()

	if len(os.Args) > 1 {
		var wg sync.WaitGroup
		resultChan := make(chan map[string]int, len(flag.Args()))

		for _, v := range flag.Args() {
			wg.Add(1)
			filePath := v
			go readFile(filePath, flags, &wg, resultChan)
		}

		go func() {
			wg.Wait()
			close(resultChan)
		}()

		for receivedMap := range resultChan {
			fmt.Println("Received map:", receivedMap)
		}

	} else {

		count := map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0}
		count = readLineByLine(os.Stdin, count, flags, map[string]bool{"file": false})

		fmt.Println("\n", count)

	}

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
	return 0
}

func readFile(filePath string, flags WcFlags, wg *sync.WaitGroup, resultChan chan map[string]int) {
	defer wg.Done()
	count := map[string]int{"words": 0, "lines": 0, "chars": 0, "errorCode": 0}
	file, err := os.Open(filePath)

	if err != nil {
		fmt.Println(err)
		return
	} else {
		info, _ := os.Stat(filePath)
		if info.IsDir() {
			fmt.Printf("wc: %s: Is a directory\n", filePath)
			count["errorCode"] = 126
			return
		}
	}
	defer file.Close()

	count = readLineByLine(file, count, flags, map[string]bool{"file": true})
	resultChan <- count
}

func readLineByLine(reader io.Reader, count map[string]int, flags WcFlags, input map[string]bool) map[string]int {
	buffReader := bufio.NewReader(reader)
	for {
		line, err := buffReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				count["errorCode"] = 0
				input["lastLine"] = true
				return countWLC(flags, count, line, input)

			} else {
				fmt.Println(err)
				count["errorCode"] = 126
			}
			break

		}
		count = countWLC(flags, count, line, input)
	}
	return count
}

func countWLC(flags WcFlags, count map[string]int, line string, input map[string]bool) map[string]int {
	if flags.c {
		count["chars"] += len(line)
	}

	if flags.w {
		words := strings.Fields(line)
		count["words"] += len(words)
	}

	if flags.l && !input["lastLine"] {
		count["lines"] += 1
	}
	return count
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
