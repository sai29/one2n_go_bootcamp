package input

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

func OpenFile(fileName string, p *parser.Parser) ([]string, error) {
	// var input io.Reader
	file, err := os.Open(fileName)
	if err != nil {
		return []string{}, fmt.Errorf("error opening the file")
	}

	defer file.Close()

	dec := json.NewDecoder(file)

	t, err := dec.Token()
	if err != nil {
		return []string{}, fmt.Errorf("error with json input")
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return []string{}, fmt.Errorf("expected [ at start of JSON array")
	}

	sqlStatements := []string{}

	for dec.More() {
		var entry parser.Oplog
		if err := dec.Decode(&entry); err != nil {
			return []string{}, fmt.Errorf("error decoding json into Oplog struct")

		} else {
			// fmt.Printf("%+v\n", entry)
			sql, err := p.GetSqlStatements(entry)
			if err != nil {
				return []string{}, err

			} else {
				sqlStatements = append(sqlStatements, sql...)

			}
		}
	}

	t, err = dec.Token()
	if err != nil {
		return []string{}, err
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		return []string{}, fmt.Errorf("expected ] at the end of JSON array")
	}
	return sqlStatements, nil

}
