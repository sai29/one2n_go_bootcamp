package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

type flags struct {
	writeToFile     bool
	caseInsensitive bool
	recursiveSearch bool
	beforeLines     int
	afterLines      int
	countLines      int
	outputFile      string
}

var (
	flagSet flags
)

func init() {
	rootCmd.Flags().BoolVarP(&flagSet.writeToFile, "send to file", "o", false, "Send grep output to file")
	rootCmd.Flags().BoolVarP(&flagSet.caseInsensitive, "Case insenstive match", "i", false, "Look to match even if case don't match")
	rootCmd.Flags().BoolVarP(&flagSet.recursiveSearch, "recursive search", "r", false, "Look to match even if case don't match")
	rootCmd.Flags().IntVarP(&flagSet.afterLines, "A", "A", 0, "Number of lines to display after match")
	rootCmd.Flags().IntVarP(&flagSet.beforeLines, "B", "B", 0, "Number of lines to display before match")
	rootCmd.Flags().IntVarP(&flagSet.countLines, "C", "C", 0, "Number of lines to display before match")
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

		// fmt.Printf("Args are %v", args)

		stdin, directory := false, false
		var fileName string

		afterLines, beforeErr := cmd.Flags().GetInt("A")
		beforeLines, afterErr := cmd.Flags().GetInt("B")

		if beforeErr != nil || afterErr != nil {
			printToStdErr(beforeErr)
			printToStdErr(afterErr)
		} else {
			flagSet.afterLines = afterLines
			flagSet.beforeLines = beforeLines
			fmt.Println("flags are ", flagSet)
		}

		if flagSet.writeToFile {
			flagSet.outputFile = args[len(args)-1]
		}

		if len(args) == 1 {
			stdin = true
			fileName = "-"
		} else {
			fileName = args[1]
			directory, _ = fileOrDirectory(args[1])
		}
		subStr := args[0]

		// if err != nil {
		// 	fmt.Println("there was an error with the path provided")
		// }

		if flagSet.recursiveSearch && directory {
			recursiveSearch(args[1], subStr)
			// generateBatchOutput(results)
			return
		} else if !directory {
			openFile(fileName, stdin, subStr)
		}

	},
}

func openFile(fileName string, stdin bool, subStr string) {
	var input io.Reader

	if stdin {
		input = os.Stdin
	} else {

		file, err := os.Open(fileName)
		if err != nil {
			// fmt.Println(err)
			printToStdErr(err)
		}
		input = file
		defer file.Close()
	}
	readFileByLine(input, subStr, fileName)
}

func readFileByLine(input io.Reader, subStr string, fileName string) {
	scanner := bufio.NewScanner(input)
	lineNum := 0
	var file *os.File
	var err error

	if flagSet.writeToFile && !flagSet.recursiveSearch {
		file, err = createFile(flagSet.outputFile)
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

		if strings.Contains(compareLine, compareSubStr) {
			output := printToTerminal(compareLine, fileName)
			// checkTypeWithReflect(output)
			// checkTypeWithReflect(file)
			if flagSet.writeToFile {
				writeStringsToFile(output, file)
			}
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error while scanning the file")
	}
}

func createFile(fileName string) (*os.File, error) {
	_, err := os.Stat(fileName)
	if err == nil {
		return nil, err
	}
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Error while creating file", err)
		return nil, err
	}
	return file, nil
}

func writeStringsToFile(line string, file *os.File) error {
	_, err := file.WriteString(line + "\n")

	if err != nil {
		fmt.Printf("%v \n", err)
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}

// func checkTypeWithReflect(value interface{}) {
// 	valueType := reflect.TypeOf(value)
// 	fmt.Println("Type:", valueType)
// }

func printToTerminal(line string, fileName string) string {
	output := ""
	if flagSet.recursiveSearch {
		output = fmt.Sprintf("%v %v", fileName, line)
	} else {
		output = fmt.Sprintf("%v", line)
	}
	fmt.Println(output)
	return output
}

func recursiveSearch(rootPath string, subStr string) {
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			openFile(path, false, subStr)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error while recursively searching through directories")
	}
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
