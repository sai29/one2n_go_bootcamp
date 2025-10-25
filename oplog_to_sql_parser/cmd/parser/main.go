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

		flagCfg.Output.OutputMethod = "file"
		flagCfg.Input.InputMethod = "file"
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
		fmt.Println("Processing interrupt...")
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

// Reader go routine - already present
// Reader go routine will parse the json into oplog struct and look for db and collection names and pass that on to dispatcher via a channel called oplogDispatchChan
// We will start another dispatcher go routine in main.go that will be blocking and reading from oplogDispatchChan
// The dispatcher go routine will check if it already has a channel(dbNameChan) and a go routine(db_worker)that listens on dbNameChan for that database - if not present it will create one and send the db.collection string via the channel(dbNameChan) to the worker(db_worker)
// Now db_worker is listening on dbNameChan channel for the "db.collection" string and it maintains a map of dbName -> [collectionChans] where collectionChans is the list of channels created for each collection
// Now db_worker checks if there is already a go routine and a channel for the collection string it received if not present it creates both(collectionNameChan) and a go routine(collection_worker) and sends the oplog to the collection_worker
// The collection_worker if already present or just created is listening on collectionNameChan for the oplog to be received and does the work of converting the oplog into sql

//
