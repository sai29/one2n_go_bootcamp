package parser

import (
	"fmt"
	"slices"
	"strings"
)

func (p *Parser) insertSql(oplog Oplog) ([]string, error) {
	output, insertValues := []string{}, []string{}

	for key, value := range oplog.Record {

		if !slices.Contains(p.tableSchemas[oplog.Namespace], key) && !nestedDocument(value) {
			p.tableSchemas[oplog.Namespace] = append(p.tableSchemas[oplog.Namespace], key)
			alterColumnType := ""
			switch value.(type) {
			case string:
				alterColumnType = "VARCHAR(255)"
			case bool:
				alterColumnType = "BOOLEAN"
			case float64, int:
				alterColumnType = "FLOAT"
			}
			output = append(output, fmt.Sprintf("ALTER TABLE %s ADD %s %s;", oplog.Namespace, key, alterColumnType))
		}
	}

	columns := append([]string{}, p.tableSchemas[oplog.Namespace]...)
	slices.Sort(columns)

	for _, column := range columns {

		if value, ok := oplog.Record[column]; ok {
			switch v := value.(type) {
			case string:
				safeVal := strings.ReplaceAll(v, "'", "''")
				insertValues = append(insertValues, fmt.Sprintf("'%s'", safeVal))
			case bool:
				insertValues = append(insertValues, fmt.Sprintf("%t", v))
			case float64, int:
				insertValues = append(insertValues, fmt.Sprintf("%v", v))
			}
		} else {
			insertValues = append(insertValues, "NULL")
		}
	}

	output = append(output, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Namespace, strings.Join(columns, ", "), strings.Join(insertValues, ", ")))
	return output, nil
}

func (p *Parser) linkedInsertSql(parentIdColumn string, parentId string, linkedTableName string, record interface{}) (string, error) {

	insertValues := []string{}

	m, ok := record.(map[string]interface{})
	if ok {
		for _, column := range p.tableSchemas[linkedTableName] {

			if value, ok := m[column]; ok {
				switch v := value.(type) {
				case string:
					safeVal := strings.ReplaceAll(v, "'", "''")
					insertValues = append(insertValues, fmt.Sprintf("'%s'", safeVal))
				case bool:
					insertValues = append(insertValues, fmt.Sprintf("%t", v))
				case float64, int:
					insertValues = append(insertValues, fmt.Sprintf("%v", v))
				default:
					insertValues = append(insertValues, fmt.Sprintf("%s", v))
				}
			} else {

				switch column {
				case parentIdColumn:
					insertValues = append(insertValues, fmt.Sprintf("'%s'", parentId))
				case "_id":
					insertValues = append(insertValues, fmt.Sprintf("'%s'", p.IdGenerator(16)))
				default:
					insertValues = append(insertValues, "NULL")
				}
			}
		}
	} else {
		return "", fmt.Errorf("error with data sent to linkedInsertSql")
	}

	output := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", linkedTableName, strings.Join(p.tableSchemas[linkedTableName], ", "), strings.Join(insertValues, ", "))
	return output, nil
}
