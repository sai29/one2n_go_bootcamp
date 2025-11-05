package input

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type FileReader struct {
	filePath string
}

func NewFileReader(filePath string) *FileReader {
	return &FileReader{filePath: filePath}
}

func (fr *FileReader) Read(streamCtx context.Context, config *config.Config, p parser.Parser,
	oplogChan chan<- parser.Oplog, errChan chan<- errors.AppError, wg *sync.WaitGroup) {

	logx.Info("Entering FileReader")
	defer close(oplogChan)

	file, err := os.Open(config.Input.InputFile)
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("error opening the file -> %v", err))
		return
	}

	defer file.Close()

	dec := json.NewDecoder(file)

	t, err := dec.Token()
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("error with json input -> %v", err))
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		errors.SendFatal(errChan, fmt.Errorf("expected [ at start of JSON array -> %v", err))
	}

	for dec.More() {
		var entry parser.Oplog
		if err := dec.Decode(&entry); err != nil {
			errors.SendWarn(errChan, fmt.Errorf("error decoding json into Oplog struct -> %v", err))
			continue
		} else {
			oplogChan <- entry
		}
	}

	t, err = dec.Token()
	if err != nil {
		errors.SendWarn(errChan, fmt.Errorf("malformed ending -> %v", err))
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		errors.SendWarn(errChan, fmt.Errorf("expected ] at the end of JSON array"))
	}

	logx.Info("Exiting FileReader")

}
