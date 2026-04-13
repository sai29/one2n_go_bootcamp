package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/bookmark"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/dispatcher"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
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
		ctx, ctxCancel := context.WithCancel(programCtx)

		handleInterrupt(ctxCancel)

		if err := config.ValidateConfig(flagCfg); err != nil {
			logx.Error("error validating input flags -> %v", err)
			return err
		}

		if err := oplogToSql(ctx); err != nil {
			logx.Error("error from oplogToSql() -> %v", err)
			return err
		}

		return nil
	},
}

func oplogToSql(ctx context.Context) error {

	oplogChan := make(chan parser.Oplog, 100)
	bookmarkChan := make(chan map[string]int, 100)
	sqlChan := make(chan input.SqlStatement, 100)
	errChan := make(chan errors.AppError, 10)

	lastSavedBk, err := bookmark.Load("data/bookmark.json")
	if err != nil {
		if err != io.EOF {
			errors.SendWarn(errChan, fmt.Errorf("couldn't decode timestamp json into bookmark struct: %s", err))
		} else {
			logx.Info("Empty file")
		}
	}

	p := parser.NewParser()
	dispatcher := dispatcher.NewDispatcher(p)

	reader := createReader(flagCfg, &lastSavedBk)
	writer, err := createWriter(flagCfg)
	if err != nil {
		return err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for err := range errChan {
			if err.Fatal {
				cancel()
				logx.Fatal("err chan -> %v", err.Err)
			}
			logx.Warn("error -> %v", err.Err)
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		logx.Info("Entering Read worker")
		reader.Read(streamCtx, flagCfg, p, oplogChan, errChan, &wg)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logx.Info("Entering Bookmark worker")
		bookmark.BookmarkWorker(streamCtx, bookmarkChan, errChan, &lastSavedBk)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logx.Info("Entering Parser worker")
		p.ParserWorker(streamCtx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logx.Info("Entering Dispatch worker")
		dispatcher.Dispatch(streamCtx, oplogChan, bookmarkChan, sqlChan, errChan, &wg, &lastSavedBk)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer.Write(streamCtx, sqlChan, errChan)
	}()

	wg.Wait()

	logx.Info("Closing all main.go channels")

	// close(bookmarkChan)
	close(errChan)

	return nil
}

func createReader(flags *config.Config, lastSavedBk *parser.Bookmark) input.Reader {
	if flags.Input.InputMethod == "file" {
		return input.NewFileReader(flags.Input.InputFile)
	}
	return input.NewMongoReader(flags.Input.InputUri, lastSavedBk)
}

func createWriter(config *config.Config) (output.Writer, error) {
	switch config.Output.OutputMethod {
	case "file":
		return output.NewFileWriter(config.Output.OutputFile)
	case "postgres":
		return output.NewPostgresWriter(config.Output.OutputUri)
	default:
		return nil, fmt.Errorf("unknown output method: %q", config.Output.OutputMethod)
	}

}

func handleInterrupt(cancel context.CancelFunc) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-interrupt
		logx.Info("Processing interrupt...")
		cancel()
	}()
}
