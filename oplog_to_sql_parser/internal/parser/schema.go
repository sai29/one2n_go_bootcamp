package parser

import (
	"fmt"
	"slices"
	"strings"
)

func (p *Parser) saveCurrentTableColumns(record any, tableName string) {

	data := record.(map[string]interface{})

	keys := make([]string, 0, len(data))
	for k, v := range data {
		switch v.(type) {
		case string, bool, int, float64:
			keys = append(keys, strings.ToLower(k))
		default:
			continue
		}
	}

	slices.Sort(keys)
	p.tableSchemas[tableName] = append(p.tableSchemas[tableName], keys...)
}

func (p *Parser) createSchemaAndTable(oplog Oplog) []string {

	output, columns := []string{}, []string{}

	parts := strings.Split(oplog.Namespace, ".")
	schema := parts[0]

	output = append(output, fmt.Sprintf("CREATE SCHEMA %s;", schema))

	var oplogRecordValue any

	nestedDocumentColumns := []string{}

	for key, value := range oplog.Record {
		switch value.(type) {
		case []interface{}:
			nestedDocumentColumns = append(nestedDocumentColumns, key)
		case map[string]interface{}:
			nestedDocumentColumns = append(nestedDocumentColumns, key)

		}
	}

	slices.Sort(nestedDocumentColumns)

	for _, value := range nestedDocumentColumns {
		switch nestedValue := oplog.Record[value].(type) {
		case []interface{}:

			tableName := fmt.Sprintf("%s_%s", oplog.Namespace, value)
			linkedTableStatements, err := p.createLinkedTable(oplog.Namespace, value, nestedValue[0])

			if err != nil {
				fmt.Println("Error generating linked table for array of nested table ->", err)
			} else {
				p.linkedTableStatements[tableName] = append(p.linkedTableStatements[tableName], linkedTableStatements)
			}

			for _, iValue := range nestedValue {
				p.interfaceToStatements(oplog, value, iValue)
			}
		case map[string]interface{}:
			linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, value, nestedValue)
			tableName := fmt.Sprintf("%s_%s", oplog.Namespace, value)

			if err != nil {
				fmt.Println("Error generating linked table ->", err)
				return []string{}
			} else {
				p.linkedTableStatements[tableName] = append(p.linkedTableStatements[tableName], linkedTableCreate)
			}
			p.interfaceToStatements(oplog, value, nestedValue)
		}
	}

	for _, key := range p.tableSchemas[oplog.Namespace] {
		oplogRecordValue = oplog.Record[key]
		switch oplogRecordValue.(type) {
		case string:
			if key == "_id" {
				columns = append(columns, "_id VARCHAR(255) PRIMARY KEY")
			} else {
				columns = append(columns, fmt.Sprintf("%s VARCHAR(255)", key))
			}
		case bool:
			columns = append(columns, fmt.Sprintf("%s BOOLEAN", key))
		case float64, int:
			columns = append(columns, fmt.Sprintf("%v FLOAT", key))
		}
	}

	columnsString := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Namespace, strings.Join(columns, ", "))
	output = append(output, columnsString)

	return output
}

func (p *Parser) interfaceToStatements(oplog Oplog, tableName string, i interface{}) {
	tableNameWithSchema := fmt.Sprintf("%s_%s", oplog.Namespace, tableName)
	parentId, ok := oplog.Record["_id"].(string)
	parentIdColumn := strings.Split(oplog.Namespace, ".")[1]

	if ok {
		linkedTableInserts, err := p.linkedInsertSql(fmt.Sprintf("%s__id", parentIdColumn), parentId, tableNameWithSchema, i)
		if err != nil {
			fmt.Println("Error generating insert statements for linked tables", err)
		} else {
			p.linkedTableStatements[tableNameWithSchema] = append(p.linkedTableStatements[tableNameWithSchema], linkedTableInserts)
		}
	}
}

func (p *Parser) createLinkedTable(nameSpace string, tableName string, data interface{}) (string, error) {

	if !p.createdTables[tableName] {
		tableMap := map[string]interface{}{}
		tableMap[tableName] = data

		columns := []string{}
		parent := strings.Split(nameSpace, ".")

		fullTableNameWithSchema := fmt.Sprintf("%s_%s", nameSpace, tableName)
		p.saveCurrentTableColumns(tableMap[tableName], fullTableNameWithSchema)
		p.createdTables[fullTableNameWithSchema] = true

		parentTableName := fmt.Sprintf("%s__id", parent[1])
		parentTable := fmt.Sprintf("%s VARCHAR(255)", parentTableName)
		columns = append(columns, "_id VARCHAR(255) PRIMARY KEY", parentTable)
		p.tableSchemas[fullTableNameWithSchema] = append(p.tableSchemas[fullTableNameWithSchema], parentTableName, "_id")
		slices.Sort(p.tableSchemas[fullTableNameWithSchema])

		m, ok := data.(map[string]interface{})
		if ok {
			for _, key := range p.tableSchemas[fullTableNameWithSchema] {
				mvalue := m[key]
				switch mvalue.(type) {
				case string:
					if key == "_id" {
						continue
					} else {
						columns = append(columns, fmt.Sprintf("%s VARCHAR(255)", key))
					}
				case bool:
					columns = append(columns, fmt.Sprintf("%s BOOLEAN", key))
				case float64, int:
					columns = append(columns, fmt.Sprintf("%v FLOAT", key))

				}
			}
		} else {
			return "", nil
		}
		// columns = append(columns, value)

		createLinkedTable := fmt.Sprintf("CREATE TABLE %s (%s);", fullTableNameWithSchema, strings.Join(columns, ", "))

		return createLinkedTable, nil
	} else {
		return "", nil
	}
}
