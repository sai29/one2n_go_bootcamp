package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type Parser struct {
	createdTables map[string]bool
	tableSchemas  map[string][]string
}

type Oplog struct {
	Op            string                 `json:"op"`
	Namespace     string                 `json:"ns"`
	Record        map[string]interface{} `json:"o"`
	UpdateColumns map[string]interface{} `json:"o2"`
	TableCreated  bool
}

func NewParser() *Parser {
	return &Parser{createdTables: make(map[string]bool), tableSchemas: make(map[string][]string)}
}

func main() {

	oplogInsertJson := []string{
		`  {
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "635b79e231d82a8ab1de863b",
      "name": "Selena Miller",
      "roll_no": 51,
      "is_graduated": false,
      "date_of_birth": "2000-01-30"
    	}
  	}`,
		`{
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "14798c213f273a7ca2cf5174",
      "name": "George Smith",
      "roll_no": 211,
      "is_graduated": true,
      "date_of_birth": "2001-03-23",
			"phone":"+91-81254966457"
    	}
  	}`,
		`{
			"op": "i",
			"ns": "test.student",
			"o": {
				"_id": "14798c213f273a7ca2cf5174",
				"name": "John Smith",
				"roll_no": 11,
				"date_of_birth": "2001-03-23",
				"phone": "+91-81254966457",
				"hourly_rate": 25
				}
			}`,
		`{
				"op": "i",
				"ns": "test.student",
				"o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "Steve Smith",
					"roll_no": 23,
					"is_graduated": true,
					"date_of_birth": "2001-03-23"
					}
				}`,
	}

	// oplogInsertJson := `{
	// "op": "i",
	// "ns": "test.student",
	// "o": {
	//   "_id": "635b79e231d82a8ab1de863b",
	//   "name": "Selena Miller",
	//   "roll_no": 51,
	//   "is_graduated": false,
	//   "date_of_birth": "2000-01-30"
	// 	}
	// }`

	// oplogUpdateJson := `{
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
	// 	}`

	// oplogDeleteJson := `{
	// 		"op": "d",
	// 		"ns": "test.student",
	// 		"o": {
	// 			"_id": "635b79e231d82a8ab1de863b"
	// 		}
	// 	}`
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
		var oplog Oplog
		jsonByte := []byte(v)
		err := json.Unmarshal(jsonByte, &oplog)
		if err != nil {
			return []string{}, fmt.Errorf("error converting json string to json -> %v", err)
		}
		fmt.Println("OPLOG when json is created ->", oplog.Record)
		sql, err := p.parseJsonStruct(oplog)
		if err != nil {
			return []string{}, fmt.Errorf("error parsing oplog struct -> %v", err)
		} else {
			output = append(output, sql...)
		}
		// fmt.Println("Oplog struct is ->", oplog)
	}
	return output, nil
}

func (p *Parser) parseJsonStruct(oplog Oplog) ([]string, error) {
	output := []string{}
	switch oplog.Op {
	case "i":

		if !p.createdTables[oplog.Namespace] {
			createSchema := createSchemaAndTable(oplog)
			p.saveCurrentTableColumns(oplog)
			p.createdTables[oplog.Namespace] = true
			output = append(output, createSchema...)

		}

		insertSql, err := p.insertSql(oplog)

		if err == nil {
			output = append(output, insertSql...)
			return output, nil

		} else {
			fmt.Println("Error in insert sql is ->", err)
			return []string{}, err
		}
		// return "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51)", nil
	case "u":
		updateSql, err := updateSql(oplog)
		if err != nil {
			output = append(output, updateSql)
			return output, nil
		} else {
			return []string{}, err
		}

		// return "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b' AND archived = false", nil
	case "d":
		deleteSql, err := deleteSql(oplog)
		if err != nil {
			output = append(output, deleteSql)
			return output, nil
		} else {
			return []string{}, err
		}

	default:
		return []string{}, fmt.Errorf("error reading collection OP value")
	}
}

func (p *Parser) saveCurrentTableColumns(oplog Oplog) {
	for key := range oplog.Record {
		p.tableSchemas[oplog.Namespace] = append(p.tableSchemas[oplog.Namespace], key)
	}
}

func createSchemaAndTable(oplog Oplog) []string {
	output := []string{}
	columns := []string{}

	parts := strings.Split(oplog.Namespace, ".")
	schema := parts[0]

	output = append(output, fmt.Sprintf("CREATE SCHEMA %s", schema))

	for key, value := range oplog.Record {
		switch value.(type) {
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

	fmt.Println("columns ->", columns)

	columnsString := fmt.Sprintf("CREATE TABLE %s ( %s )", oplog.Namespace, strings.Join(columns, ", "))
	output = append(output, columnsString)

	return output
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
			output = append(output, fmt.Sprintf("ALTER TABLE %s ADD %s %s", oplog.Namespace, key, alterColumnType))
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
			default:
				insertValues = append(insertValues, fmt.Sprintf("%s", v))
			}
		} else {
			fmt.Println("Null value columns are ->", column)
			insertValues = append(insertValues, "NULL")
		}

	}

	tableName := getQualifiedTableName(oplog.Namespace)
	output = append(output, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(p.tableSchemas[oplog.Namespace], ","), strings.Join(insertValues, ",")))

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
