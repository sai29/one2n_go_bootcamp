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

type fileCount struct {
	flags               WcFlags
	fileName            string
	words, chars, lines int
	errorCode           int
}

type AdvancedReader interface {
	io.Reader
	SetSource(source io.Reader)
}

type CustomReader struct {
	reader io.Reader
}

func NewCustomReader(r io.Reader) *CustomReader {
	return &CustomReader{
		reader: r,
	}
}

func (cr *CustomReader) Read(p []byte) (n int, err error) {
	return cr.reader.Read(p)
}

func (cr *CustomReader) SetSource(source io.Reader) {
	cr.reader = source
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

func run() int {

	ParseFlags()
	flags := NewWcFlags()

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
		customReader := NewCustomReader(os.Stdin)
		count = readLineByLine(customReader, count, flags, map[string]bool{"file": false, "singleLine": false})

		fmt.Println(count)

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

	customReader := NewCustomReader(file)

	count = readLineByLine(customReader, count, flags, map[string]bool{"file": true, "singLine": false})
	resultChan <- count
}

func readLineByLine(customReader *CustomReader, count map[string]int, flags WcFlags, input map[string]bool) map[string]int {
	reader := bufio.NewReader(customReader)
	loop := 0
	for {
		loop += 1
		fmt.Printf("Count is %v\n", loop)
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				count["errorCode"] = 0
				if !input["file"] && input["singleLine"] {
					fmt.Printf("line is %v\n", line)
					return countWLC(flags, count, line, input)
				}
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

	if flags.l && !input["singleLine"] {
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
