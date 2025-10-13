package output

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
)

type FileWriter struct {
	uri string
}

func NewFileWriter(uri string) *FileWriter {
	return &FileWriter{uri: uri}
}

func (fr *FileWriter) Write(ctx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- error) {
	var file *os.File
	var err error
	file, err = openOrCreateFile(fr.uri)
	if err != nil {
		fmt.Println("Error creating file", err)
		errChan <- fmt.Errorf("failed to create or open file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer func() {
		err = writer.Flush()
		if err != nil {
			fmt.Println("Error flushing file ->", err)
			return
		}
	}()

	for stmt := range sqlChan {
		select {
		case <-ctx.Done():
			return
		default:

		}
		// Need to find a better way to handle nil values than this line below.
		if stmt.Sql != "" {
			_, err = file.WriteString(stmt.Sql + "\n")
			if err != nil {
				fmt.Printf("error writing to output file -> %v\n", err)
				errChan <- fmt.Errorf("error writing to file -> %w", err)
			}

		}

	}
}

func openOrCreateFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error opening/creating file -> %s\n err: %s", fileName, err)
	}
	return file, nil
}
