package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

const openFileLimit = 10

type matchCountByFile struct {
	fileName string
	count    int
}

type matchCounts struct {
	total []matchCountByFile
}
type flags struct {
	writeToFile     bool
	caseInsensitive bool
	recursiveSearch bool
	beforeLines     int
	afterLines      int
	countLines      bool
	matchLineCount  int
	outputFile      string
}

var (
	flagSet          flags
	matchCountsTotal matchCounts
	// fileChan         = make(chan string, 100)
	resultChan       = make(chan string, 100)
	maxOpenFileLimit = make(chan int, openFileLimit)
	wg               sync.WaitGroup
	mutex            sync.Mutex
)

func init() {
	rootCmd.Flags().BoolVarP(&flagSet.writeToFile, "send to file", "o", false, "Send grep output to file")
	rootCmd.Flags().BoolVarP(&flagSet.caseInsensitive, "Case insenstive match", "i", false, "Look to match even if case don't match")
	rootCmd.Flags().BoolVarP(&flagSet.recursiveSearch, "recursive search", "r", false, "Look to match even if case don't match")
	rootCmd.Flags().IntVarP(&flagSet.afterLines, "A", "A", 0, "Number of lines to display after match")
	rootCmd.Flags().IntVarP(&flagSet.beforeLines, "B", "B", 0, "Number of lines to display before match")
	rootCmd.Flags().BoolVarP(&flagSet.countLines, "C", "C", false, "Count of matching lines")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		printToStdErr(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "grep",
	Short: "grep is used to find the presence of an input string",
	Long:  "grep is given an input of a STDIN/file/directory and will confirm the presence of an input string if it is present in the entity we are checking on.",
	Run: func(cmd *cobra.Command, args []string) {

		afterLines, beforeErr := cmd.Flags().GetInt("A")
		beforeLines, afterErr := cmd.Flags().GetInt("B")

		if beforeErr != nil || afterErr != nil {
			printToStdErr(beforeErr)
			printToStdErr(afterErr)
		} else {
			flagSet.afterLines = afterLines
			flagSet.beforeLines = beforeLines
		}

		if flagSet.writeToFile {
			flagSet.outputFile = args[len(args)-1]
		}

		// if len(args) == 1 {
		// 	stdin = true
		// 	fileName = "-"
		// } else {
		// 	fileName = args[1]
		// 	directory, _ = fileOrDirectory(args[1])
		// }
		subStr := args[0]

		fileSearch(args[1], subStr)

		if flagSet.countLines {
			if !flagSet.recursiveSearch {
				fmt.Println(flagSet.matchLineCount)
			} else if flagSet.recursiveSearch {
				generateCountByFile(matchCountsTotal)
			}
		}

	},
}

func fileSearch(rootPath string, subStr string) {
	fmt.Println("coming here")
	// for i := 0; i < openFileLimit; i++ {
	// 	wg.Add(1)
	// 	go worker(subStr)
	// }

	// go resultCollector()

	fmt.Println("Root path is", rootPath)

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			go worker(path, subStr)
			// fileChan <- path
			// openFile(path, false, subStr)
		}
		return nil
	})

	close(resultChan)
	wg.Wait()

	if err != nil {
		fmt.Println(err)
		// fmt.Printf("Error while recursively searching through directories")
	}
}

func worker(path string, subStr string) {
	// defer close(fileChan)
	// defer close(resultChan)
	maxOpenFileLimit <- 1

	defer func() {
		wg.Done()
		<-maxOpenFileLimit
	}()

	processFile(path, subStr)

	// for {
	// 	filePath, ok := <-fileChan
	// 	if ok {
	// 		fmt.Println("Its okay")
	// 	}
	// 	if !ok {
	// 		fmt.Println("Error sending file path to the channel")
	// 	}
	// 	results := processFile(filePath, subStr)
	// 	for _, result := range results {
	// 		resultChan <- result
	// 	}
	// }

}

func processFile(path string, subStr string) []string {
	fmt.Println("path is ", path, subStr)
	return make([]string, 1)
}

func generateCountByFile(matches matchCounts) {
	for _, value := range matches.total {
		fmt.Printf("\n%v:%v", value.fileName, value.count)
	}
}

func openFile(fileName string, stdin bool, subStr string) {
	var input io.Reader

	if stdin {
		input = os.Stdin
	} else {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println(err)
		}
		input = file
		defer file.Close()
	}
	readFileByLine(input, subStr, fileName)
}

func readFileByLine(input io.Reader, subStr string, fileName string) {
	scanner := bufio.NewScanner(input)
	var file *os.File
	var err error
	matchLineCount := 0

	var buffer []string
	var afterCount int
	var linesSinceLastMatch int
	var firstMatch bool
	var separatorNeeded bool

	if flagSet.writeToFile {
		file, err = openOrCreateFile(flagSet.outputFile)
		if err != nil {
			fmt.Println("Error creating file", err)
		}
		defer file.Close()
	}
	for scanner.Scan() {
		line := scanner.Text()

		compareLine := line
		compareSubStr := subStr

		if flagSet.caseInsensitive {
			compareLine = strings.ToLower(line)
			compareSubStr = strings.ToLower(subStr)
		}

		if flagSet.beforeLines > 0 || flagSet.afterLines > 0 {
			if strings.Contains(compareLine, compareSubStr) {

				if separatorNeeded {
					fmt.Println("--")
					separatorNeeded = false
				}

				for _, l := range buffer {
					printCurrentLine(l)
				}
				buffer = nil

				printCurrentLine(line)
				afterCount = flagSet.afterLines
				linesSinceLastMatch = 0
				firstMatch = true
			} else {

				linesSinceLastMatch++
				if afterCount > 0 {
					printCurrentLine(line)
					afterCount--
				} else {
					if flagSet.beforeLines > 0 {
						if len(buffer) == flagSet.beforeLines {
							buffer = buffer[1:]
						}
						buffer = append(buffer, line)
					}

					if firstMatch && linesSinceLastMatch > flagSet.beforeLines+flagSet.afterLines {
						separatorNeeded = true
					}
				}
			}
		}

		if strings.Contains(compareLine, compareSubStr) {
			matchLineCount++
			output := printMatches(line, fileName)
			if flagSet.writeToFile {
				writeStringsToFile(output, file)
			}
		}
	}

	if flagSet.countLines {
		if flagSet.recursiveSearch {
			matchCountsTotal.total = append(matchCountsTotal.total, matchCountByFile{fileName: fileName, count: matchLineCount})
		} else {
			flagSet.matchLineCount += matchLineCount
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error while scanning the file")
	}
}

func printCurrentLine(line string) {
	fmt.Printf("%s \n", line)
}

func openOrCreateFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Error opening/creating file:", err)
		return nil, err
	}
	return file, nil
}

func writeStringsToFile(line string, file *os.File) error {
	_, err := file.WriteString(line + "\n")

	if err != nil {
		fmt.Printf("wrintStringsToFile err %v \n", err)
		return fmt.Errorf("failed to write to file: %w", err)
	}
	return nil
}

func printMatches(line string, fileName string) string {
	output := ""
	if flagSet.recursiveSearch {
		output = fmt.Sprintf("%v %v", fileName, line)
	} else {
		output = fmt.Sprintf("%v", line)
	}
	if !flagSet.countLines && !flagSet.writeToFile && !(flagSet.beforeLines > 0 || flagSet.afterLines > 0) {
		fmt.Println(output)
	}
	return output
}

func fileOrDirectory(path string) (bool, error) {
	directory := false
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, fmt.Errorf("path does not exist")
		}
		fmt.Println("Error:", err)
		return false, fmt.Errorf("error while processing file/directory at path")
	}

	if fileInfo.IsDir() {
		directory = true
	} else {
		directory = false
	}
	return directory, nil
}

func printToStdErr(err error) {
	fmt.Fprint(os.Stderr, err)
}
