package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "tree",
	Short: "Print file directory structure in the form of a tree at given input directory",
	Long:  "Print the directory structure at the given directory with various flags to modify the output",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Hello tree!")
	},
}
