package input

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/bookmark"
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
			bk, err := bookmark.Load("bookmark.json")
			if err != nil {
				if err != io.EOF {
					errChan <- fmt.Errorf("couldn't decode timestamp json into bookmark struct: %s", err)
				} else {
					fmt.Println("Empty file")
				}
			}

			bookmarkTimestamp := bk.LastTS.T
			bookmarkIncrement := bk.LastTS.I

			currentTimestamp := int(entry.TimeStamp["T"].(float64))
			currentIncrement := int(entry.TimeStamp["I"].(float64))

			if bk.LastTS.T == 0 ||
				(currentTimestamp > bookmarkTimestamp) ||
				(currentTimestamp == bk.LastTS.T && currentIncrement > bookmarkIncrement) {
				sql, err := p.GenerateSql(entry)
				// fmt.Println("Sql is ->", sql)
				if err != nil {
					errChan <- fmt.Errorf("error from GetSqlStatements -> %v", err)
				} else {

					for _, stmt := range sql {
						sqlChan <- SqlStatement{Sql: stmt, IsBoundary: false}
					}
					sqlChan <- SqlStatement{IsBoundary: true}

					if err := bookmark.SaveBookmark("bookmark.json", currentTimestamp, currentIncrement); err != nil {
						fmt.Println("error saving bookmark timestamp", err)
						errChan <- fmt.Errorf("error saving bookmark timestamp -> %s", err)
					} else {
						fmt.Println("saved bookmark successfully ->", currentTimestamp)
					}

				}
			} else {
				fmt.Println("Skipping oplog because of timestamp")
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
