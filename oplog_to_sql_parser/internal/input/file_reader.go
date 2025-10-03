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
	sqlChan chan<- []string, errChan chan<- error) {
	defer close(sqlChan)
	defer close(errChan)
	// var input io.Reader
	file, err := os.Open(config.InputFile)
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

	sqlStatements := []string{}

	for dec.More() {
		var entry parser.Oplog
		if err := dec.Decode(&entry); err != nil {
			errChan <- fmt.Errorf("error decoding json into Oplog struct")

		} else {
			// fmt.Printf("%+v\n", entry)
			sql, err := p.GetSqlStatements(entry)
			if err != nil {
				errChan <- fmt.Errorf("error from GetSqlStatements -> %v", err)

			} else {
				sqlStatements = append(sqlStatements, sql...)

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

	fmt.Println("Sending data via sqlChan to main")
	sqlChan <- sqlStatements

}
