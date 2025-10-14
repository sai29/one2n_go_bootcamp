package parser

import (
	"fmt"
	"slices"
	"strings"
)

func (p *parser) saveCurrentTableColumns(record any, tableName string) {

	if !p.createdTables[tableName] {

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

func (p *parser) createSchemaAndTable(oplog Oplog) map[string][]string {
	output := make(map[string][]string)
	columns := []string{}

	//Schema CREATE

	if !p.createdTables[oplog.Namespace] {

		parts := strings.Split(oplog.Namespace, ".")
		schema := parts[0]

		output["main_table"] = append(output["main_table"], fmt.Sprintf("CREATE SCHEMA %s;", schema))

	}

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

	if len(nestedDocumentColumns) > 0 {
		for _, nestedColumn := range nestedDocumentColumns {
			linkedTableName := fmt.Sprintf("%s_%s", oplog.Namespace, nestedColumn)

			switch nestedValue := oplog.Record[nestedColumn].(type) {

			case []interface{}:

				//Nested table CREATE

				// fmt.Printf("Nested nestedColumn is -> +%v\n", nestedValue)

				if !p.createdTables[linkedTableName] {
					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue[0])

					if err != nil {
						fmt.Println("Error generating linked table for array of nested table ->", err)
					} else {
						if linkedTableCreate != "" {
							output["linked_table"] = append(output["linked_table"], linkedTableCreate)
							p.createdTables[linkedTableName] = true

						}
					}
				}

				//Nested table Insert

				for _, iValue := range nestedValue {
					output["linked_table"] = append(output["linked_table"], p.interfaceToStatements(oplog, nestedColumn, iValue))
				}
			case map[string]interface{}:

				//Nested table CREATE

				if !p.createdTables[linkedTableName] {

					linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, nestedColumn, nestedValue)

					if err != nil {
						fmt.Println("Error generating linked table ->", err)
						return make(map[string][]string)
					} else {
						if linkedTableCreate != "" {
							output["linked_table"] = append(output["linked_table"], linkedTableCreate)
							p.createdTables[linkedTableName] = true

						}
					}
				}

				//Nested table Insert
				output["linked_table"] = append(output["linked_table"], p.interfaceToStatements(oplog, nestedColumn, nestedValue))

			}
		}

	}

	// Table Create (Top level)

	if !p.createdTables[oplog.Namespace] {

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
		output["main_table"] = append(output["main_table"], columnsString)
		p.createdTables[oplog.Namespace] = true
	}

	return output
}

func (p *parser) interfaceToStatements(oplog Oplog, tableName string, i interface{}) string {
	tableNameWithSchema := fmt.Sprintf("%s_%s", oplog.Namespace, tableName)
	parentId, ok := oplog.Record["_id"].(string)
	parentIdColumn := strings.Split(oplog.Namespace, ".")[1]

	if ok {
		linkedTableInserts, err := p.linkedInsertSql(fmt.Sprintf("%s__id", parentIdColumn), parentId, tableNameWithSchema, i)
		// fmt.Println("Linked table inserts is", linkedTableInserts)
		if err != nil {
			fmt.Println("Error generating insert statements for linked tables", err)
		} else {
			return linkedTableInserts
		}
	}
	return ""
}

func (p *parser) createLinkedTable(nameSpace string, tableName string, data interface{}) (string, error) {

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

// Need to decouple schema and table creation for top level collection from nested table creation and insert statements creation
// Need to check if table is already present before creating schema and table but that shouldn't prevent from nested table insert statement creation
// Similarly need to check if linked table is already present before creating linked table" - this is already being done
// Even if linked table and schema are present we still need to generate insert statements for nested table data
// We need a way to append the nested inserts into the output being sent to the writer

// WHY IS CREATE SCHEMA AND CREATE TABLE AT TOP LEVEL SEPARATED BY CODE FOR NESTED TABLE CREATE AND INSERT?? BAD CODE - ALL IN SAME FUNCTION AS WELL
