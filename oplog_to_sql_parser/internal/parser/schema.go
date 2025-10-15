package parser

import (
	"fmt"
	"slices"
	"strings"
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

func (p *parser) createSchemaAndTable(oplog Oplog) map[string][]string {

	output := make(map[string][]string)

	schemaStmt := p.createSchema(oplog)
	if schemaStmt != "" {
		output["main_table"] = append(output["main_table"], schemaStmt)
	}

	nestedColumns := nestedColumns(oplog.Record)

	nestedStmts := p.nestedColumnOperations(nestedColumns, oplog)
	if len(nestedStmts) > 0 {
		output["linked_table"] = append(output["linked_table"], nestedStmts...)
	}

	columnsString := p.createTable(oplog)
	if columnsString != "" {
		output["main_table"] = append(output["main_table"], columnsString)
		p.markTableCreated(oplog.Namespace)
	}

	return output
}

func (p *parser) nestedColumnOperations(nestedColumns []string, oplog Oplog) []string {
	nestedStmts := []string{}
	if len(nestedColumns) > 0 {
		for _, nestedColumn := range nestedColumns {
			linkedTableName := fmt.Sprintf("%s_%s", oplog.Namespace, nestedColumn)

			switch nestedValue := oplog.Record[nestedColumn].(type) {
			case []interface{}:
				if p.tableNotPresent(linkedTableName) {
					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue[0])

					if err != nil {
						fmt.Println("Error generating linked table for array of nested table ->", err)
						return []string{}
					} else {
						if linkedTableCreate != "" {
							nestedStmts = append(nestedStmts, linkedTableCreate)
							p.markTableCreated(linkedTableName)
						}
					}
				}

				for _, iValue := range nestedValue {
					nestedStmts = append(nestedStmts, p.generateLinkedInsertSql(oplog, nestedColumn, iValue))
				}

			case map[string]interface{}:
				if p.tableNotPresent(linkedTableName) {
					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue)
					if err != nil {
						fmt.Println("Error generating linked table ->", err)
						return []string{}
					} else {
						if linkedTableCreate != "" {
							nestedStmts = append(nestedStmts, linkedTableCreate)
							p.markTableCreated(linkedTableName)

						}
					}
				}

				nestedStmts = append(nestedStmts, p.generateLinkedInsertSql(oplog, nestedColumn, nestedValue))
			}
		}

	}
	return nestedStmts
}

func (p *parser) createTable(oplog Oplog) string {
	columns, createdTable := []string{}, ""
	var oplogRecordValue any

	if p.tableNotPresent(oplog.Namespace) {
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

		createdTable += fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Namespace, strings.Join(columns, ", "))
	}
	return createdTable
}

func (p *parser) generateLinkedInsertSql(oplog Oplog, tableName string, i interface{}) string {
	tableNameWithSchema := fmt.Sprintf("%s_%s", oplog.Namespace, tableName)
	parentIdValue, ok := oplog.Record["_id"].(string)
	parentIdColumn := strings.Split(oplog.Namespace, ".")[1]

	if ok {

		parentIdColumnName := fmt.Sprintf("%s__id", parentIdColumn)

		linkedTableInserts, err := p.linkedInsertSql(parentIdColumnName, parentIdValue, tableNameWithSchema, i)
		if err != nil {
			fmt.Println("Error generating insert statements for linked tables", err)
		} else {
			return linkedTableInserts
		}
	}
	return ""
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
		createLinkedTableStmts := fmt.Sprintf("CREATE TABLE %s (%s);", fullTableNameWithSchema, strings.Join(columns, ", "))

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

// Need to decouple schema and table creation for top level collection from nested table creation and insert statements creation
// Need to check if table is already present before creating schema and table but that shouldn't prevent from nested table insert statement creation
// Similarly need to check if linked table is already present before creating linked table" - this is already being done
// Even if linked table and schema are present we still need to generate insert statements for nested table data
// We need a way to append the nested inserts into the output being sent to the writer

// WHY IS CREATE SCHEMA AND CREATE TABLE AT TOP LEVEL SEPARATED BY CODE FOR NESTED TABLE CREATE AND INSERT?? BAD CODE - ALL IN SAME FUNCTION AS WELL
