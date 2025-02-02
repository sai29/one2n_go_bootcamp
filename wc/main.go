package main

import (
	"bytes"
	"errors"
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

type totalCounter struct {
	lines, words, chars int
	mu                  sync.Mutex
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

func main() {

	parseFlags()
	flags := newWcFlags()
	counter := &totalCounter{}

	args := flag.Args()
	if len(os.Args) == 1 {
		args = append(args, "-")
	}
	var wg sync.WaitGroup
	maxOpenFileLimit := make(chan int, openFileLimit)

	for _, fileName := range args {
		wg.Add(1)
		go func(fileName string) {
			fileIntake(fileName, flags, &wg, maxOpenFileLimit, counter)
		}(fileName)
	}

	wg.Wait()

	if len(args) > 1 {
		file := fileCount{
			words:    counter.words,
			lines:    counter.lines,
			chars:    counter.chars,
			fileName: "total",
		}
		output, err := generateOutput(file, flags)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}
		fmt.Fprint(os.Stdout, output)
	}
}

func fileIntake(fileName string, flags WcFlags, wg *sync.WaitGroup, fileLimit chan int, counter *totalCounter) {
	var input io.Reader
	fileLimit <- 1

	defer func() {
		wg.Done()
		<-fileLimit
	}()

	line := make(chan []byte)
	errChan := make(chan error)

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

	go readFileByLine(input, line, errChan)
	var fileStats fileCount
	fileStats.fileName = fileName

	processLine(line, errChan, &fileStats)

	counter.words += fileStats.chars
	counter.lines += fileStats.lines
	counter.chars += fileStats.chars

	output, err := generateOutput(fileStats, flags)

	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}
	fmt.Fprint(os.Stdout, output)
}

func (c *totalCounter) Add(fileCount *fileCount) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.words += fileCount.chars
	c.lines += fileCount.lines
	c.chars += fileCount.chars
}

func processLine(line <-chan []byte, errChan <-chan error, fileCount *fileCount) error {
	for {
		select {
		case err := <-errChan:
			if err != nil {
				fmt.Fprint(os.Stderr, err)
				return nil
			}
		case line, ok := <-line:
			if !ok {
				fmt.Fprint(
					os.Stderr,
					errors.New("error receiving from line channel in processLine"),
				)
				return nil
			}
			updateCount(line, fileCount)
		}
	}
}

func updateCount(line []byte, counter *fileCount) {

	if len(line) > 0 && line[len(line)-1] == '\n' {
		counter.lines += 1
	}
	counter.words += len(bytes.Fields(line))
	counter.chars += len(line)

}

func readFileByLine(input io.Reader, line chan<- []byte, errChan chan<- error) {
	defer close(line)
	defer close(errChan)

	buffer := make([]byte, 256*1024)
	var lineBuffer []byte

	for {
		buffSizeVal, err := input.Read(buffer)

		if err != nil && err != io.EOF {
			errChan <- fmt.Errorf("%v", err)
			return
		}

		for i := 0; i < buffSizeVal; i++ {
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

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
