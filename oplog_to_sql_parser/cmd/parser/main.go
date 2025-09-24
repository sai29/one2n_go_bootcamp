package main

import (
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/output"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"

	"github.com/spf13/cobra"
)

var flagCfg = &FlagConfig{}

type FlagConfig struct {
	InputFile  string
	OutputFile string
	InputUri   string
	OutputUri  string
}

func init() {
	rootCmd.Flags().StringVar(&flagCfg.InputFile, "input-file", "", "Input json oplog file")
	rootCmd.Flags().StringVar(&flagCfg.OutputFile, "output-file", "", "Output sql file to write to")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		parser.PrintToStdErr(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "oplog to sql parser",
	Short: "Convert mongodb oplog to sql statements",
	Long:  "Process json or direct streamed input from mongodb and convert it into sql statements or send them to a postgres db",
	RunE: func(cmd *cobra.Command, args []string) error {
		parser := parser.NewParser()
		// openFile(flagCfg.InputFile, parser)

		// sql, err := parser.decodeJSONString(oplogInsertJson)
		sql, err := input.OpenFile(flagCfg.InputFile, parser)

		if err != nil {
			return err
		} else {
			// fmt.Println("rootCmd sql is ->", sql)
			output.WriteToFileActions(sql, flagCfg.OutputFile)

		}
		return nil
	},
}
