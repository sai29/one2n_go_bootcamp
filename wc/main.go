package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
)

type WcFlags struct {
	w bool
	l bool
	c bool
}

const openFileLimit = 10

type fileCount struct {
	fileName            string
	words, chars, lines int
	error               error
}

var (
	lineCount, wordCount, charCount int
)

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

func main() {

	parseFlags()
	flags := newWcFlags()

	args := flag.Args()
	if len(os.Args) == 1 {
		args = append(args, "-")
	}
	var wg sync.WaitGroup
	maxOpenFileLimit := make(chan int, openFileLimit)

	for _, fileName := range args {
		wg.Add(1)
		go fileIntake(fileName, flags, &wg, maxOpenFileLimit)
	}

	wg.Wait()

	if len(args) > 1 {
		file := fileCount{
			words:    wordCount,
			lines:    lineCount,
			chars:    charCount,
			fileName: "total",
		}
		result, err := generateOutput(file, flags)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}
		fmt.Println(result)
	}
}

func fileIntake(fileName string, flags WcFlags, wg *sync.WaitGroup, fileLimit chan int) {
	fileLimit <- 1

	defer func() {
		wg.Done()
		<-fileLimit
	}()

	line := make(chan []byte)
	errChan := make(chan error)

	go readFileByLine(fileName, line, errChan)
	result := count(flags, line, errChan)
	wordCount += result.chars
	lineCount += result.lines
	charCount += result.chars
	result.fileName = fileName

	output, err := generateOutput(result, flags)

	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}
	fmt.Println(output)
}

func generateOutput(count fileCount, flags WcFlags) (string, error) {
	output := ""

	if count.error != nil {
		return "", count.error
	}

	if flags.c {
		output += fmt.Sprintf("%8v", count.chars)
	}

	if flags.w {
		output += fmt.Sprintf("%8v", count.words)
	}

	if flags.l {
		output += fmt.Sprintf("%8v", count.lines)
	}

	if !flags.w && !flags.l && !flags.c {
		output += fmt.Sprintf("%8v", count.chars)
		output += fmt.Sprintf("%8v", count.words)
		output += fmt.Sprintf("%8v", count.lines)
	}

	if count.fileName == "-" {
		output += "\n"
	} else {
		output += fmt.Sprintf(" " + count.fileName + "\n")
	}
	return output, nil
}

func readFileByLine(fileName string, line chan<- []byte, errChan chan<- error) {
	defer close(line)
	defer close(errChan)
	var input io.Reader
	if fileName == "-" {
		input = os.Stdin
	} else {
		file, err := os.Open(fileName)
		if err != nil {
			errChan <- fmt.Errorf("%v", err)
		}
		defer file.Close()
		input = file
	}

	buffer := make([]byte, 256*1024)
	var lineBuffer []byte

	for {
		n, err := input.Read(buffer)

		if err != nil && err != io.EOF {
			errChan <- fmt.Errorf("%v", err)
			return
		}

		for i := 0; i < n; i++ {
			lineBuffer = append(lineBuffer, buffer[i])
			if buffer[i] == '\n' {
				line <- lineBuffer
				lineBuffer = lineBuffer[:0]
			}
		}

		if err == io.EOF {
			if len(lineBuffer) > 0 {
				line <- lineBuffer
			}
			return
		}
	}
}

func count(flags WcFlags, line <-chan []byte, errChan <-chan error) fileCount {
	// fmt.Printf("Line is %v", line)
	var count fileCount
	for {
		select {
		case err := <-errChan:
			if err != nil {
				count.error = err
				return count
			}
		case line, ok := <-line:
			// fmt.Println(string(line))
			if !ok {
				return count
			}
			if len(line) > 0 && line[len(line)-1] == '\n' {
				count.lines += 1
			}
			count.words += len(bytes.Fields(line))
			count.chars += len(line)
		}
	}
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
