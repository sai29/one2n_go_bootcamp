package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type GrepResult struct {
	lines []string
}

type GrepResults struct {
	results []GrepResult
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		printToStderr(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "grep",
	Short: "grep is used to find the presence of an input string",
	Long:  "grep is given an input of a STDIN/file/directory and will confirm the presence of an input string if it is present in the entity we are checking on.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			args = []string{"-"}
		}

		openFile(args)
	},
}

func openFile(args []string) {

	subStr := args[0]
	fileName := args[len(args)-1]
	file, err := os.Open(fileName)
	if err != nil {
		printToStderr(err)
	}
	input := file
	defer file.Close()

	result := readFileByLine(input, subStr)

	for _, v := range result.lines {
		fmt.Printf("%v\n", v)
	}

}

func readFileByLine(input io.Reader, subStr string) GrepResult {
	grepResult := GrepResult{}
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, subStr) {
			grepResult.lines = append(grepResult.lines, line)
		}
	}
	return grepResult
}

func printToStderr(err error) {
	fmt.Fprint(os.Stderr, err)
}

func printToStdOut(str string) {
	fmt.Fprint(os.Stdout, str)
}
