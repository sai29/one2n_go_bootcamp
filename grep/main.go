package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type grepResult struct {
	lines []string
}

type flags struct {
	sendToFile      bool
	caseInsensitive bool
}

var (
	flagSet flags
)

func init() {
	rootCmd.Flags().BoolVarP(&flagSet.sendToFile, "send to file", "o", false, "Send grep output to file")
	rootCmd.Flags().BoolVarP(&flagSet.caseInsensitive, "Case insenstive match", "i", false, "Look to match even if case don't match")

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

		result := openFile(args, stdin)
		sendToFile(result, args[len(args)-1])

		generateOutput(result)
	},
}

func openFile(args []string, stdin bool) grepResult {
	var input io.Reader
	subStr := args[0]
	if stdin {
		input = os.Stdin
	} else {
		fileName := args[1]
		file, err := os.Open(fileName)
		if err != nil {
			printToStdErr(err)
		}
		input = file
		defer file.Close()

	}
	return readFileByLine(input, subStr)
}

func readFileByLine(input io.Reader, subStr string) grepResult {
	grepResult := grepResult{}
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

func sendToFile(result grepResult, fileName string) error {
	if flagSet.sendToFile {
		_, err := os.Stat(fileName)
		if err == nil {
			printToStdErr(err)
		}
		file, err := os.Create(fileName)
		if err != nil {
			printToStdErr(err)
		}
		defer file.Close()

		for _, line := range result.lines {
			_, err := file.WriteString(line + "\n")
			if err != nil {
				return fmt.Errorf("failed to write to file: %w", err)
			}
		}
	}
	return nil
}

func generateOutput(output grepResult) {
	for _, v := range output.lines {
		fmt.Printf("\n%v", v)
	}
}

func printToStdErr(err error) {
	fmt.Fprint(os.Stderr, err)
}

func printToStdOut(str string) {
	fmt.Fprint(os.Stdout, str)
}
