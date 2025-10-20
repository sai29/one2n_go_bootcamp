package parser

import (
	"fmt"
)

type Parser interface {
	GenerateSql(oplog Oplog) ([]string, error)
}

type parser struct {
	createdTables         map[string]bool
	tableSchemas          map[string][]string
	linkedTableStatements map[string][]string
	IdGenerator           func(int) string
}

type Oplog struct {
	Op            string                 `json:"op"`
	Namespace     string                 `json:"ns"`
	Record        map[string]interface{} `json:"o"`
	UpdateColumns map[string]interface{} `json:"o2"`
	TableCreated  bool
	TimeStamp     map[string]interface{} `json:"ts"`
}

type Bookmark struct {
	LastTS struct {
		T int `json:"T"`
		I int `json:"I"`
	} `json:"last_ts"`
	LastNamespace string `json:"last_namespace"`
}

func NewParser() Parser {
	return &parser{createdTables: make(map[string]bool), tableSchemas: make(map[string][]string),
		linkedTableStatements: make(map[string][]string), IdGenerator: randString}
}

func (p *parser) GenerateSql(oplog Oplog) ([]string, error) {
	sql, err := p.HandleOplog(oplog)
	if err != nil {
		return []string{}, fmt.Errorf("error parsing oplog struct -> %v", err)
	} else {
		return sql, nil
	}
}

func (p *parser) HandleOplog(oplog Oplog) ([]string, error) {
	output := []string{}
	switch oplog.Op {
	case "i":
		p.saveCurrentTableColumns(oplog.Record, oplog.Namespace)

		statements, err := p.createSchemaAndTable(oplog)
		if err != nil {
			return nil, err
		} else {
			output = append(output, statements["main_table"]...)
		}

		alterStmts, insertColumns := p.checkForNewColumns(oplog)
		output = append(output, alterStmts...)

		insertSql, err := insertSql(oplog, insertColumns)

		if err != nil {
			fmt.Println("Error in insert sql is ->", err)
			return []string{}, err
		} else {

			output = append(output, insertSql...)
			output = append(output, statements["linked_table"]...)
			return output, nil
		}

	case "u":
		updateSql, err := updateSql(oplog)
		if err != nil {
			return []string{}, err
		} else {
			output = append(output, updateSql)
			return output, nil
		}

	case "d":
		deleteSql, err := deleteSql(oplog)
		if err != nil {
			return []string{}, err
		} else {
			output = append(output, deleteSql)
			return output, nil
		}

	default:
		return []string{}, fmt.Errorf("error reading collection OP value")
	}
}
