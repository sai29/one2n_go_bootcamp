package output

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type FileWriter struct {
	uri string
}

func NewFileWriter(uri string) *FileWriter {
	return &FileWriter{uri: uri}
}

func (fr *FileWriter) Write(ctx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- errors.AppError) {
	var file *os.File
	var err error
	file, err = parser.OpenOrCreateFile(fr.uri)
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("failed to create or open file: %w", err))
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer func() {
		err = writer.Flush()
		if err != nil {
			logx.Info("error flushing file -> %s", err)
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

			if _, err = file.WriteString(stmt.Sql + "\n"); err != nil {
				errors.SendWarn(errChan, fmt.Errorf("error writing to file sql %v -> %w", stmt.Sql, err))
			}

		}
	}
}
