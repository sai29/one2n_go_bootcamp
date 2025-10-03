package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/output"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"

	"github.com/spf13/cobra"
)

var flagCfg = &config.Config{}

func init() {
	rootCmd.Flags().StringVar(&flagCfg.InputFile, "input-file", "", "Input json oplog file")
	rootCmd.Flags().StringVar(&flagCfg.OutputFile, "output-file", "", "Output sql file to write to")

	rootCmd.Flags().StringVar(&flagCfg.InputUri, "input-uri", "", "Input json oplog uri")
	rootCmd.Flags().StringVar(&flagCfg.OutputUri, "output-uri", "", "Output postgres db uri")

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

		programCtx := context.Background()
		streamCtx, streamCancel := context.WithCancel(programCtx)

		handleInterrupt(streamCancel)

		flagCfg.OutputMethod = "file"
		flagCfg.InputMethod = "db"
		// sql, err := parser.decodeJSONString(oplogInsertJson)

		if err := fetchSqlFromInputSource(streamCtx); err != nil {
			fmt.Printf("Error from fetchSqlFromInputSource: %s", err)
			return err
		}

		return nil
	},
}

func handleInterrupt(cancel context.CancelFunc) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-interrupt
		fmt.Println("Shutting down program...")
		cancel()
	}()
}

func fetchSqlFromInputSource(streamCtx context.Context) error {
	parser := parser.NewParser()
	sqlChan := make(chan []string)
	errChan := make(chan error)
	reader := createReader(flagCfg.InputFile, flagCfg.InputUri)

	fmt.Println("Before calling go routine ->")

	go reader.Read(streamCtx, flagCfg, parser, sqlChan, errChan)

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
		case sql, ok := <-sqlChan:
			if !ok {
				return fmt.Errorf("error from sql Statments channel")
			}
			fmt.Println("Receiving data ->", sql)
			fmt.Println("Sending to writer ->")
			writer := createWriter(flagCfg, sql)
			writer.Write(sql)
			return nil

		}
	}

	// sql, err := reader.Read(streamCtx, flagCfg, parser, sqlChan, errChan)
	// if err != nil {
	// 	return err
	// }
}

func createReader(file, uri string) input.Reader {
	if file != "" {
		return input.NewFileReader(file)
	}

	return input.NewMongoReader(uri)
}

func createWriter(config *config.Config, sql []string) output.Writer {
	if config.OutputFile != "" {
		return output.NewFileWriter(config.OutputFile)
	}

	return output.NewPostgresWriter(config.OutputUri)
}
