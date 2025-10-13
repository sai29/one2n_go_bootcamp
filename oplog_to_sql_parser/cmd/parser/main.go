package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/output"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"

	"github.com/spf13/cobra"
)

var flagCfg = &config.Config{}

func init() {
	rootCmd.Flags().StringVar(&flagCfg.Input.InputFile, "input-file", "", "Input json oplog file")
	rootCmd.Flags().StringVar(&flagCfg.Output.OutputFile, "output-file", "", "Output sql file to write to")

	rootCmd.Flags().StringVar(&flagCfg.Input.InputUri, "input-uri", "", "Input json oplog uri")
	rootCmd.Flags().StringVar(&flagCfg.Output.OutputUri, "output-uri", "", "Output postgres db uri")

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

		flagCfg.Output.OutputMethod = "db"
		flagCfg.Input.InputMethod = "db"
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
	sqlChan := make(chan input.SqlStatement)
	errChan := make(chan error)
	reader := createReader(flagCfg.Input.InputFile, flagCfg.Input.InputUri)

	// fmt.Println("Start fetchSqlFromInputSource")
	writer := createWriter(flagCfg)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		go reader.Read(streamCtx, flagCfg, parser, sqlChan, errChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer.Write(streamCtx, sqlChan, errChan)
	}()

	go func() {
		for error := range errChan {
			fmt.Printf("error from error channel -> %s\n", error)
		}
	}()

	wg.Wait()

	return nil
}

func createReader(file, uri string) input.Reader {
	if file != "" {
		return input.NewFileReader(file)
	}
	return input.NewMongoReader(uri)
}

func createWriter(config *config.Config) output.Writer {
	if config.Output.OutputMethod == "file" && config.Output.OutputFile != "" {
		return output.NewFileWriter(config.Output.OutputFile)
	}
	return output.NewPostgresWriter(config.Output.OutputUri)
}
