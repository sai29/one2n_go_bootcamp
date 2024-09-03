package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type TreeNode struct {
	Name     string
	IsDir    bool
	Children []*TreeNode
}

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
		fmt.Println("args are", args)
		generateTree(args[0])
	},
}

func generateTree(directory string) (TreeNode, error) {
	return TreeNode{
		Name:  ".",
		IsDir: true,
		Children: []*TreeNode{
			{Name: "go.mod", IsDir: false},
			{Name: "go.sum", IsDir: false},
			{Name: "main_test", IsDir: false},
			{Name: "main.go", IsDir: false},
		},
	}, nil
}
