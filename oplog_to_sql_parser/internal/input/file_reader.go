package input

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type FileReader struct {
	filePath string
}

func NewFileReader(filePath string) *FileReader {
	return &FileReader{filePath: filePath}
}

func (fr *FileReader) Read(streamCtx context.Context, config *config.Config, p parser.Parser,
	oplogChan chan<- parser.Oplog, errChan chan<- error, wg *sync.WaitGroup) {

	fmt.Println("inside FileReader")
	defer close(oplogChan)

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
			// fmt.Println("Sending oplog from file to db worker")
			oplogChan <- entry
		}
	}

	t, err = dec.Token()
	if err != nil {
		errChan <- fmt.Errorf("%v", err)
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		errChan <- fmt.Errorf("expected ] at the end of JSON array")
	}

	fmt.Println("Exiting file reader")

}
