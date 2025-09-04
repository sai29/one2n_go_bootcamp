package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Oplog struct {
	Op            string                 `json:"op"`
	Namespace     string                 `json:"ns"`
	Record        map[string]interface{} `json:"o"`
	UpdateColumns map[string]interface{} `json:"o2"`
}

func main() {
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

	oplogUpdateJson := `{
   "op": "u",
   "ns": "test.student",
   "o": {
      "$v": 2,
      "diff": {
         "u": {
            "is_graduated": false,
						"is_enrolled": true
        	 }
      	}
   		},
    "o2": {
      "_id": "635b79e231d82a8ab1de863b",
			"age": 25
   		}
		}`

	sql, err := decodeJSONString(oplogUpdateJson)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Sql is ->", sql)

}

func decodeJSONString(jsonOplog string) (string, error) {

	var oplog Oplog
	jsonByte := []byte(jsonOplog)

	err := json.Unmarshal(jsonByte, &oplog)
	if err != nil {

		fmt.Println("Error converting json string to json value ->", err)
		return "", fmt.Errorf("error converting json string to json -> %v", err)
	}

	sql, err := parseJsonStruct(oplog)
	if err != nil {
		fmt.Println("Error parsing json struct ->", err)
	}

	fmt.Println("Oplog struct is ->", oplog)
	fmt.Println("Sql generated is ->", sql)
	return sql, nil
}

func parseJsonStruct(oplog Oplog) (string, error) {
	switch oplog.Op {
	case "i":
		return insertSql(oplog)
		// return "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51)", nil
	case "u":
		return updateSql(oplog)
		// return "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b' AND archived = false", nil
	default:
		return "", fmt.Errorf("error reading collection OP value")
	}

}

func insertSql(oplog Oplog) (string, error) {
	output := ""
	columnNames, values := []string{}, []string{}

	for key, value := range oplog.Record {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", key))

		switch v := value.(type) {
		case string:
			safeVal := strings.ReplaceAll(v, "'", "''")
			values = append(values, fmt.Sprintf("'%s'", safeVal))
		case bool:
			values = append(values, fmt.Sprintf("%t", v))
		case float64, int:
			values = append(values, fmt.Sprintf("%v", v))
		default:
			values = append(values, fmt.Sprintf("%v", v))
		}
	}

	tableName := getQualifiedTableName(oplog.Namespace)
	output += fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columnNames, ","), strings.Join(values, ","))
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
