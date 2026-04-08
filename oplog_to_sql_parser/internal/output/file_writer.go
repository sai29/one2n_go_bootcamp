package output

import (
	"bufio"
	"context"
	"fmt"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type FileWriter struct {
	uri string
}

func NewFileWriter(uri string) (*FileWriter, error) {
	if uri == "" {
		return nil, fmt.Errorf("invalid or empty uri")
	}
	return &FileWriter{uri: uri}, nil
}

func (fr *FileWriter) Write(ctx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- errors.AppError) {
	file, err := parser.OpenOrCreateFile(fr.uri)
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("failed to create or open file: %w", err))
		return
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer func() {
		if flushErr := bw.Flush(); flushErr != nil {
			errors.SendWarn(errChan, fmt.Errorf("error flushing file -> %w", flushErr))
			return
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logx.Info("Cancel called from FileWriter")
			return
		case stmt, ok := <-sqlChan:

			if !ok {
				return
			}

			if stmt.Sql == "" {
				continue
			}

			if _, err = bw.WriteString(stmt.Sql + "\n"); err != nil {
				errors.SendWarn(errChan, fmt.Errorf("error writing to file sql %q -> %w", stmt.Sql, err))
			}
		}
	}
}
