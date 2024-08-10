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

type grepResult struct {
	lines    []string
	fileName string
}

type BatchResult struct {
	results []grepResult
}

type flags struct {
	writeToFile     bool
	caseInsensitive bool
	recursiveSearch bool
}

var (
	flagSet flags
)

func init() {
	rootCmd.Flags().BoolVarP(&flagSet.writeToFile, "send to file", "o", false, "Send grep output to file")
	rootCmd.Flags().BoolVarP(&flagSet.caseInsensitive, "Case insenstive match", "i", false, "Look to match even if case don't match")
	rootCmd.Flags().BoolVarP(&flagSet.recursiveSearch, "recursive search", "r", false, "Look to match even if case don't match")
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

		stdin := false
		if len(args) == 1 {
			stdin = true
		}

		subStr := args[0]
		fileName := args[1]

		directory, err := fileOrDirectory(args[1])
		if err != nil {
			fmt.Println("there was an error with the path provided")
		}

		if flagSet.recursiveSearch && directory {
			results := recursiveSearch(args[1], subStr)
			batchFileActions(results, args[len(args)-1])
			generateBatchOutput(results)
			return
		} else {

			result := openFile(fileName, stdin, subStr)
			fileActions(result, args[len(args)-1])
			generateOutput(result)
		}

	},
}

func openFile(fileName string, stdin bool, subStr string) grepResult {
	var input io.Reader

	if stdin {
		input = os.Stdin
	} else {

		file, err := os.Open(fileName)
		if err != nil {
			printToStdErr(err)
		}
		input = file
		defer file.Close()
	}
	return readFileByLine(input, subStr, fileName)
}

func readFileByLine(input io.Reader, subStr string, fileName string) grepResult {
	grepResult := grepResult{fileName: fileName}
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if flagSet.caseInsensitive {
			line = strings.ToLower(line)
			subStr = strings.ToLower(subStr)
		}
		if strings.Contains(line, subStr) {
			grepResult.lines = append(grepResult.lines, line)
		}
	}
	return grepResult
}

func recursiveSearch(rootPath string, subStr string) BatchResult {
	finalResult := BatchResult{results: []grepResult{}}
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			result := openFile(path, false, subStr)
			finalResult.results = append(finalResult.results, result)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error")
	}
	return finalResult
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

func createFile(fileName string) (*os.File, error) {
	_, err := os.Stat(fileName)
	if err == nil {
		return nil, err
	}
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return file, nil

}

func batchFileActions(batchResult BatchResult, fileName string) {
	if flagSet.writeToFile {
		file, err := createFile(fileName)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		for _, result := range batchResult.results {
			writeStringsToFile(result, file)
		}
	}
}

func fileActions(result grepResult, fileName string) error {
	if flagSet.writeToFile {
		file, err := createFile(fileName)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		return writeStringsToFile(result, file)
	}
	return nil
}

func writeStringsToFile(result grepResult, file *os.File) error {
	for _, line := range result.lines {
		_, err := file.WriteString(line + "\n")

		if err != nil {
			fmt.Printf("Error at writeString to file is %v \n", err)
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	return nil
}

func generateOutput(output grepResult) {
	for _, v := range output.lines {
		fmt.Printf("\n%v", v)
	}
}

func generateBatchOutput(batchResults BatchResult) {
	for _, v := range batchResults.results {
		if len(v.lines) != 0 {
			for _, line := range v.lines {
				fmt.Printf("\n%v %v", v.fileName, line)
			}
		}
	}
}

func printToStdErr(err error) {
	fmt.Fprint(os.Stderr, err)
}

func printToStdOut(str string) {
	fmt.Fprint(os.Stdout, str)
}
