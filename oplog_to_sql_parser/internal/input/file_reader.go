package input

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type FileReader struct {
	filePath string
}

func NewFileReader(filePath string) *FileReader {
	return &FileReader{filePath: filePath}
}

func (fr *FileReader) Read(streamCtx context.Context, config *config.Config, p *parser.Parser,
	sqlChan chan<- SqlStatement, errChan chan<- error) {

	defer close(sqlChan)
	defer close(errChan)

	file, err := os.Open(config.Input.InputFile)
	if err != nil {
		errChan <- fmt.Errorf("error opening the file -> %v", err)
	}

	defer file.Close()

	dec := json.NewDecoder(file)

	t, err := dec.Token()
	if err != nil {
		errChan <- fmt.Errorf("error with json input -> %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		errChan <- fmt.Errorf("expected [ at start of JSON array")
	}

	for dec.More() {
		var entry parser.Oplog
		if err := dec.Decode(&entry); err != nil {
			errChan <- fmt.Errorf("error decoding json into Oplog struct")
			continue
		} else {
			// fmt.Printf("%+v\n", entry)
			sql, err := p.GetSqlStatements(entry)
			// fmt.Println("Sql is ->", sql)
			if err != nil {
				errChan <- fmt.Errorf("error from GetSqlStatements -> %v", err)

			} else {

				for _, stmt := range sql {
					sqlChan <- SqlStatement{Sql: stmt, IsBoundary: false}
				}

				sqlChan <- SqlStatement{IsBoundary: true}
			}
		}
	}

	t, err = dec.Token()
	if err != nil {
		errChan <- fmt.Errorf("%v", err)
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		errChan <- fmt.Errorf("expected ] at the end of JSON array")
	}

}
