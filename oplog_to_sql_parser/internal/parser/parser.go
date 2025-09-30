package parser

import (
	"fmt"
)

type Parser struct {
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
}

func NewParser() *Parser {
	return &Parser{createdTables: make(map[string]bool), tableSchemas: make(map[string][]string),
		linkedTableStatements: make(map[string][]string), IdGenerator: randString}
}

func (p *Parser) GetSqlStatements(oplog Oplog) ([]string, error) {
	sql, err := p.ParseJsonStruct(oplog)
	if err != nil {
		return []string{}, fmt.Errorf("error parsing oplog struct -> %v", err)
	} else {
		return sql, nil
	}
}

func (p *Parser) ParseJsonStruct(oplog Oplog) ([]string, error) {
	output := []string{}
	switch oplog.Op {
	case "i":

		if !p.createdTables[oplog.Namespace] {
			p.saveCurrentTableColumns(oplog.Record, oplog.Namespace)

			createSchema := p.createSchemaAndTable(oplog)
			p.createdTables[oplog.Namespace] = true
			output = append(output, createSchema...)

		}
		insertSql, err := p.insertSql(oplog)

		if err == nil {
			output = append(output, insertSql...)

			if len(p.linkedTableStatements) != 0 {
				for _, value := range p.linkedTableStatements {
					output = append(output, value...)
				}
			}
			return output, nil

		} else {
			fmt.Println("Error in insert sql is ->", err)
			return []string{}, err
		}
	case "u":
		updateSql, err := p.updateSql(oplog)
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
