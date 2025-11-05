package parser

import (
	"fmt"
	"slices"
	"strings"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
)

func (p *parser) saveCurrentTableColumns(record any, tableName string) {

	if p.tableNotPresent(tableName) {

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
}

func nestedColumns(record map[string]interface{}) []string {
	nestedDocumentColumns := []string{}

	for key, value := range record {
		switch value.(type) {
		case []interface{}:
			nestedDocumentColumns = append(nestedDocumentColumns, key)
		case map[string]interface{}:
			nestedDocumentColumns = append(nestedDocumentColumns, key)
		}
	}

	slices.Sort(nestedDocumentColumns)
	return nestedDocumentColumns
}

func (p *parser) createSchema(oplog Oplog) string {

	if p.tableNotPresent(oplog.Namespace) {
		parts := strings.Split(oplog.Namespace, ".")
		schema := parts[0]

		return fmt.Sprintf("CREATE SCHEMA %s;", schema)
	}
	return ""

}

func (p *parser) createSchemaAndTable(oplog Oplog) (map[string][]string, error) {

	output := make(map[string][]string)

	schemaStmt := p.createSchema(oplog)
	if schemaStmt != "" {
		output["main_table"] = append(output["main_table"], schemaStmt)
	}

	nestedColumns := nestedColumns(oplog.Record)

	nestedStmts, err := p.generateNestedTableStatements(nestedColumns, oplog)
	if err != nil {
		return nil, err
	} else if len(nestedStmts) > 0 {
		output["linked_table"] = append(output["linked_table"], nestedStmts...)
	}

	columnsString := p.createTable(oplog)
	if columnsString != "" {
		output["main_table"] = append(output["main_table"], columnsString)
		p.markTableCreated(oplog.Namespace)
	}

	return output, nil
}

func (p *parser) generateNestedTableStatements(nestedColumns []string, oplog Oplog) ([]string, error) {
	nestedStmts := []string{}
	if len(nestedColumns) > 0 {
		for _, nestedColumn := range nestedColumns {
			linkedTableName := fmt.Sprintf("%s_%s", oplog.Namespace, nestedColumn)

			switch nestedValue := oplog.Record[nestedColumn].(type) {
			case []interface{}:
				if p.tableNotPresent(linkedTableName) {
					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue[0])

					if err != nil {
						return nil, err
					} else {
						if linkedTableCreate != "" {
							nestedStmts = append(nestedStmts, linkedTableCreate)
							p.markTableCreated(linkedTableName)
						}
					}
				}

				for _, iValue := range nestedValue {
					nestedStmt, err := p.generateLinkedInsertSql(oplog, nestedColumn, iValue)
					if err != nil {
						return nil, err
					} else if nestedStmt != "" {
						nestedStmts = append(nestedStmts, nestedStmt)
					}
				}

			case map[string]interface{}:
				if p.tableNotPresent(linkedTableName) {
					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue)
					if err != nil {
						logx.Error("Error generating linked table -> %v", err)
						return nil, err
					} else {
						if linkedTableCreate != "" {
							nestedStmts = append(nestedStmts, linkedTableCreate)
							p.markTableCreated(linkedTableName)

						}
					}
				}

				nestedStmt, err := p.generateLinkedInsertSql(oplog, nestedColumn, nestedValue)
				if err != nil {
					return nil, err
				} else if nestedStmt != "" {
					nestedStmts = append(nestedStmts, nestedStmt)
				}
			}
		}

	}
	return nestedStmts, nil
}

func (p *parser) createTable(oplog Oplog) string {
	columns, createdTable := []string{}, ""
	var oplogRecordValue any

	if p.tableNotPresent(oplog.Namespace) {
		for _, key := range p.tableSchemas[oplog.Namespace] {
			oplogRecordValue = oplog.Record[key]
			colDef := inferSQLType(key, oplogRecordValue)
			if colDef != "" {
				columns = append(columns, colDef)
			}
		}

		createdTable += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", oplog.Namespace, strings.Join(columns, ", "))
	}
	return createdTable
}

func (p *parser) generateLinkedInsertSql(oplog Oplog, tableName string, i interface{}) (string, error) {
	tableNameWithSchema := fmt.Sprintf("%s_%s", oplog.Namespace, tableName)
	parentIdValue, ok := oplog.Record["_id"].(string)
	parentIdColumn := strings.Split(oplog.Namespace, ".")[1]

	if ok {
		parentIdColumnName := fmt.Sprintf("%s__id", parentIdColumn)

		linkedTableInserts, err := p.linkedInsertSql(parentIdColumnName, parentIdValue, tableNameWithSchema, i)
		if err != nil {
			return "", fmt.Errorf("error generating insert statements for linked tables: %s", err)
		} else {
			return linkedTableInserts, nil
		}
	}
	return "", nil
}

func (p *parser) createLinkedTable(nameSpace string, tableName string, data interface{}) (string, error) {

	if p.tableNotPresent(tableName) {

		tableMap := map[string]interface{}{}
		tableMap[tableName] = data

		columns := []string{}
		parent := strings.Split(nameSpace, ".")

		fullTableNameWithSchema := fmt.Sprintf("%s_%s", nameSpace, tableName)

		p.saveCurrentTableColumns(tableMap[tableName], fullTableNameWithSchema)
		p.markTableCreated(fullTableNameWithSchema)

		parentTableName := fmt.Sprintf("%s__id", parent[1])
		parentTable := fmt.Sprintf("%s VARCHAR(255)", parentTableName)
		columns = append(columns, "_id VARCHAR(255) PRIMARY KEY", parentTable)

		p.tableSchemas[fullTableNameWithSchema] = append(p.tableSchemas[fullTableNameWithSchema], parentTableName, "_id")
		slices.Sort(p.tableSchemas[fullTableNameWithSchema])

		m, ok := data.(map[string]interface{})
		if ok {
			for _, key := range p.tableSchemas[fullTableNameWithSchema] {
				mvalue := m[key]
				colDef := inferSQLType(key, mvalue)
				if colDef != "" {
					columns = append(columns, inferSQLType(key, mvalue))
				}
			}
		} else {
			return "", nil
		}
		createLinkedTableStmts := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", fullTableNameWithSchema, strings.Join(columns, ", "))

		return createLinkedTableStmts, nil
	}
	return "", nil

}

func (p *parser) tableNotPresent(tableName string) bool {
	return !p.createdTables[tableName]
}

func (p *parser) markTableCreated(tableName string) {
	p.createdTables[tableName] = true
}
