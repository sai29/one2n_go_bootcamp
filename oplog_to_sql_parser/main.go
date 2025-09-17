package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strings"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Parser struct {
	createdTables         map[string]bool
	tableSchemas          map[string][]string
	linkedTableStatements map[string][]string
	oplog                 Oplog
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
		linkedTableStatements: make(map[string][]string), oplog: Oplog{}}
}

func main() {

	oplogInsertJson := []string{
		`{
			"op": "i",
			"ns": "test.student",
			"o": {
				"_id": "635b79e231d82a8ab1de863b",
				"name": "Selena Miller",
				"roll_no": 51,
				"is_graduated": false,
				"date_of_birth": "2000-01-30",
				"address": [
					{
						"line1": "481 Harborsburgh",
						"zip": "89799"
					},
					{
						"line1": "329 Flatside",
						"zip": "80872"
					}
				],
				"phone": {
					"personal": "7678456640",
					"work": "8130097989"
				}
			}
		}`,
	}

	// oplogInsertJson := []string{
	// 	`  {
	//   "op": "i",
	//   "ns": "test.student",
	//   "o": {
	//     "_id": "635b79e231d82a8ab1de863b",
	//     "name": "Selena Miller",
	//     "roll_no": 51,
	//     "is_graduated": false,
	//     "date_of_birth": "2000-01-30"
	//   	}
	// 	}`,
	// 	`{
	//   "op": "i",
	//   "ns": "test.student",
	//   "o": {
	//     "_id": "14798c213f273a7ca2cf5174",
	//     "name": "George Smith",
	//     "roll_no": 211,
	//     "is_graduated": true,
	//     "date_of_birth": "2001-03-23",
	// 		"phone":"+91-81254966457"
	//   	}
	// 	}`,
	// 	`{
	// 		"op": "i",
	// 		"ns": "test.student",
	// 		"o": {
	// 			"_id": "14798c213f273a7ca2cf5174",
	// 			"name": "John Smith",
	// 			"roll_no": 11,
	// 			"date_of_birth": "2001-03-23",
	// 			"phone": "+91-81254966457",
	// 			"hourly_rate": 25
	// 			}
	// 		}`,
	// 	`{
	// 			"op": "i",
	// 			"ns": "test.student",
	// 			"o": {
	// 				"_id": "14798c213f273a7ca2cf5174",
	// 				"name": "Steve Smith",
	// 				"roll_no": 23,
	// 				"is_graduated": true,
	// 				"date_of_birth": "2001-03-23"
	// 				}
	// 			}`,
	// }

	// oplogInsertJson := []string{`{
	// "op": "i",
	// "ns": "test.student",
	// "o": {
	//   "_id": "635b79e231d82a8ab1de863b",
	//   "name": "Selena Miller",
	//   "roll_no": 51,
	//   "is_graduated": false,
	//   "date_of_birth": "2000-01-30"
	// 	}
	// }`}

	// oplogUpdateJson := []string{`{
	//  "op": "u",
	//  "ns": "test.student",
	//  "o": {
	//     "$v": 2,
	//     "diff": {
	//        "u": {
	//           "is_graduated": false,
	// 					"is_enrolled": true
	//       	 }
	//     	}
	//  		},
	//   "o2": {
	//     "_id": "635b79e231d82a8ab1de863b",
	// 		"age": 25
	//  		}
	// 	}`}

	// oplogDeleteJson := []string{`{
	// 		"op": "d",
	// 		"ns": "test.student",
	// 		"o": {
	// 			"_id": "635b79e231d82a8ab1de863b"
	// 		}
	// 	}`}

	parser := NewParser()

	sql, err := parser.decodeJSONString(oplogInsertJson)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Sql is ->", sql)

}

func (p *Parser) decodeJSONString(jsonOplog []string) ([]string, error) {

	output := []string{}
	for _, v := range jsonOplog {
		jsonByte := []byte(v)
		err := json.Unmarshal(jsonByte, &p.oplog)
		if err != nil {
			return []string{}, fmt.Errorf("error converting json string to json -> %v", err)
		}
		// fmt.Println("OPLOG when json is created ->", oplog.Record)
		sql, err := p.parseJsonStruct()
		if err != nil {
			return []string{}, fmt.Errorf("error parsing oplog struct -> %v", err)
		} else {
			output = append(output, sql...)
		}
	}
	return output, nil
}

func (p *Parser) parseJsonStruct() ([]string, error) {
	output := []string{}
	switch p.oplog.Op {
	case "i":

		if !p.createdTables[p.oplog.Namespace] {
			createSchema := p.createSchemaAndTable(p.oplog)
			p.saveCurrentTableColumns(p.oplog.Record, p.oplog.Namespace)
			p.createdTables[p.oplog.Namespace] = true
			output = append(output, createSchema...)

		}

		insertSql, err := p.insertSql(p.oplog)

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
		fmt.Println("Are we coming here? ->", p.oplog)
		updateSql, err := updateSql(p.oplog)
		if err != nil {
			return []string{}, err
		} else {
			output = append(output, updateSql)
			return output, nil

		}

	case "d":
		deleteSql, err := deleteSql(p.oplog)
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

func (p *Parser) saveCurrentTableColumns(record interface{}, tableName string) {

	v := record.(map[string]interface{})
	for key := range v {
		p.tableSchemas[tableName] = append(p.tableSchemas[tableName], key)
	}
}

func (p *Parser) createSchemaAndTable(oplog Oplog) []string {
	output := []string{}
	columns := []string{}

	parts := strings.Split(oplog.Namespace, ".")
	schema := parts[0]

	output = append(output, fmt.Sprintf("CREATE SCHEMA %s;", schema))

	for key, value := range oplog.Record {
		switch v := value.(type) {
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
		case []interface{}:
			tableName := fmt.Sprintf("%s_%s", oplog.Namespace, key)

			linkedTableStatements, err := p.createLinkedTable(oplog.Namespace, key, v[0])

			if err != nil {
				fmt.Println("Error generating linked table for array of nested table ->", err)
			} else {
				p.linkedTableStatements[tableName] = append(p.linkedTableStatements[tableName], linkedTableStatements)
			}

			for _, iValue := range v {
				p.interfaceToStatements(key, iValue)
			}
		case interface{}:

			linkedTableCreate, err := p.createLinkedTable(oplog.Namespace, key, v)
			tableName := fmt.Sprintf("%s_%s", oplog.Namespace, key)

			if err != nil {
				fmt.Println("Error generating linked table ->", err)
				return []string{}
			} else {
				p.linkedTableStatements[tableName] = append(p.linkedTableStatements[tableName], linkedTableCreate)
			}

			p.interfaceToStatements(key, v)
		}
	}

	columnsString := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Namespace, strings.Join(columns, ", "))
	output = append(output, columnsString)

	return output
}

func (p *Parser) interfaceToStatements(tableName string, i interface{}) {
	tableNameWithSchema := fmt.Sprintf("%s_%s", p.oplog.Namespace, tableName)
	parentId, ok := p.oplog.Record["_id"].(string)
	parentIdColumn := strings.Split(p.oplog.Namespace, ".")[1]

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

		m, ok := data.(map[string]interface{})
		if ok {
			for key, mvalue := range m {
				switch mvalue.(type) {
				case string:
					// fmt.Println("key and value is ->", key, mvalue)
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
					insertValues = append(insertValues, fmt.Sprintf("'%s'", randString(16)))
				default:
					insertValues = append(insertValues, "NULL")
				}
			}
		}
	} else {
		return "", fmt.Errorf("error with data sent to linkedInsertSql")
	}

	output := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", linkedTableName, strings.Join(p.tableSchemas[linkedTableName], ", "), strings.Join(insertValues, ","))

	return output, nil
}

func (p *Parser) insertSql(oplog Oplog) ([]string, error) {
	output := []string{}
	insertValues := []string{}

	for key, value := range oplog.Record {

		if !slices.Contains(p.tableSchemas[oplog.Namespace], key) {
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

	for _, column := range p.tableSchemas[oplog.Namespace] {
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
			fmt.Println("Null value columns are ->", column)
			insertValues = append(insertValues, "NULL")
		}
	}

	output = append(output, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Namespace, strings.Join(p.tableSchemas[oplog.Namespace], ", "), strings.Join(insertValues, ",")))

	return output, nil
}

func updateSql(oplog Oplog) (string, error) {
	output := ""
	fieldsWithValues, whereColumnsPaired := []string{}, []string{}

	diff, ok := oplog.Record["diff"].(map[string]interface{})

	if !ok {
		fmt.Println("Error fetching diff for UPDATE statement")
		return "", fmt.Errorf("error fetching diff for UPDATE statement")
	}

	fieldsToUpdate, ok := diff["u"].(map[string]interface{})

	if !ok {
		fmt.Println("Error fetching u for UPDATE statement")
		return "", fmt.Errorf("error fetching u for UPDATE statement")
	}

	whereColumns := oplog.UpdateColumns

	whereColumnsPaired = appendedColumnsAndValues(whereColumnsPaired, whereColumns)
	fieldsWithValues = appendedColumnsAndValues(fieldsWithValues, fieldsToUpdate)

	tableName := getQualifiedTableName(oplog.Namespace)
	output += fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(fieldsWithValues, ", "), strings.Join(whereColumnsPaired, " AND "))

	return output, nil
}

func deleteSql(oplog Oplog) (string, error) {
	output := ""
	whereColumnsPaired := []string{}

	whereColumnsPaired = appendedColumnsAndValues(whereColumnsPaired, oplog.Record)

	tableName := getQualifiedTableName(oplog.Namespace)
	output += fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, strings.Join(whereColumnsPaired, " AND "))
	return output, nil
}

func appendedColumnsAndValues(appendSlice []string, columnsMap map[string]interface{}) []string {
	for key, value := range columnsMap {
		switch v := value.(type) {
		case string:
			safeVal := strings.ReplaceAll(v, "'", "''")
			appendSlice = append(appendSlice, fmt.Sprintf("\"%s\" = '%s'", key, safeVal))
		case bool:
			appendSlice = append(appendSlice, fmt.Sprintf("\"%s\" = %t", key, v))
		case float64, int:
			appendSlice = append(appendSlice, fmt.Sprintf("\"%s\" = %v", key, v))
		default:
			appendSlice = append(appendSlice, fmt.Sprintf("\"%s\" = %v", key, v))
		}
	}
	return appendSlice
}

func getQualifiedTableName(tableName string) string {
	parts := strings.SplitN(tableName, ".", 2)
	if len(parts) < 2 {
		return fmt.Sprintf("\"%s\"", tableName) // just table, no schema
	}
	schema, table := parts[0], parts[1]
	return fmt.Sprintf("\"%s\".\"%s\"", schema, table)
}

func randString(n int) string {

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
