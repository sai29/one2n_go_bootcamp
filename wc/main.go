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

type totalCounter struct {
	lines, words, chars int
	mu                  sync.Mutex
}

func newWcFlags() WcFlags {
	if !isFlagPassed("l") && !isFlagPassed("w") && !isFlagPassed("c") {
		return WcFlags{
			l: true,
			c: true,
			w: true,
		}
	}
	return WcFlags{
		l: isFlagPassed("l"),
		c: isFlagPassed("c"),
		w: isFlagPassed("w"),
	}
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
			result := fileIntake(fileName, &wg, maxOpenFileLimit)
			if result.error != nil {
				fmt.Fprintln(os.Stderr, result.error)
				return
			} else {
				counter.updateTotalCount(result)
				output, err := generateOutput(result, flags)

				if err != nil {
					fmt.Fprint(os.Stderr, err)
					return
				}
				fmt.Fprint(os.Stdout, output)

			}

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
		output, err := generateOutput(&file, flags)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}
		fmt.Fprint(os.Stdout, output)
	}
}

func fileIntake(fileName string, wg *sync.WaitGroup, fileLimit chan int) *fileCount {
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
			return &fileCount{error: fmt.Errorf("wc: %s: No such file or directory", fileName)}
		}
		defer file.Close()
		input = file
	}

	go readFileByLine(input, line, errChan)

	var fileStats fileCount
	fileStats.fileName = fileName

	return processLine(line, errChan, &fileStats)
}

func (c *totalCounter) updateTotalCount(fileCount *fileCount) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.words += fileCount.chars
	c.lines += fileCount.lines
	c.chars += fileCount.chars
}

func processLine(line <-chan []byte, errChan <-chan error, fileCount *fileCount) *fileCount {
	for {
		select {
		case err := <-errChan:
			if err != nil {
				fmt.Fprint(os.Stderr, err)
				fileCount.error = err
				return fileCount
			}
		case line, ok := <-line:
			if !ok {
				return fileCount
			}
			updateFileCount(line, fileCount)
		}
	}
}

func updateFileCount(line []byte, counter *fileCount) {

	if len(line) > 0 && line[len(line)-1] == '\n' {
		counter.lines++
	}
	counter.words += len(bytes.Fields(line))
	counter.chars += len(line)

}

func readFileByLine(input io.Reader, line chan<- []byte, errChan chan<- error) {
	defer close(line)
	defer close(errChan)

	buffer := make([]byte, 256*1024)
	lineBuffer := make([]byte, 0, 256)

	for {
		buffSizeVal, err := input.Read(buffer)

		if err != nil && err != io.EOF {
			errChan <- fmt.Errorf("%v", err)
			return
		}

		for i := 0; i < buffSizeVal; i++ {
			lineBuffer = append(lineBuffer, buffer[i])
			if buffer[i] == '\n' {
				lineCopy := make([]byte, len(lineBuffer))
				copy(lineCopy, lineBuffer)
				line <- lineCopy
				lineBuffer = lineBuffer[:0]
			}
		}

		if err == io.EOF {
			if len(lineBuffer) > 0 {
				lineCopy := make([]byte, len(lineBuffer))
				copy(lineCopy, lineBuffer)
				line <- lineCopy
			}
			return
		}
	}
}

func generateOutput(count *fileCount, flags WcFlags) (string, error) {
	output := ""

	if count.error != nil {
		return "", count.error
	}

	if flags.l {
		output += fmt.Sprintf("%8v", count.lines)
	}

	if flags.w {
		output += fmt.Sprintf("%8v", count.words)
	}

	if flags.c {
		output += fmt.Sprintf("%8v", count.chars)
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
