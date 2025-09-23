package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var flagCfg = &FlagConfig{}

type Parser struct {
	createdTables         map[string]bool
	tableSchemas          map[string][]string
	linkedTableStatements map[string][]string
	idGenerator           func(int) string
}

type Oplog struct {
	Op            string                 `json:"op"`
	Namespace     string                 `json:"ns"`
	Record        map[string]interface{} `json:"o"`
	UpdateColumns map[string]interface{} `json:"o2"`
	TableCreated  bool
}

type FlagConfig struct {
	InputFile  string
	OutputFile string
	InputUri   string
	OutputUri  string
}

func NewParser() *Parser {
	return &Parser{createdTables: make(map[string]bool), tableSchemas: make(map[string][]string),
		linkedTableStatements: make(map[string][]string), idGenerator: randString}
}

func init() {
	rootCmd.Flags().StringVar(&flagCfg.InputFile, "input-file", "", "Input json oplog file")
	rootCmd.Flags().StringVar(&flagCfg.OutputFile, "output-file", "", "Output sql file to write to")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		printToStdErr(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "oplog to sql parser",
	Short: "Convert mongodb oplog to sql statements",
	Long:  "Process json or direct streamed input from mongodb and convert it into sql statements or send them to a postgres db",
	RunE: func(cmd *cobra.Command, args []string) error {
		parser := NewParser()
		// openFile(flagCfg.InputFile, parser)

		// sql, err := parser.decodeJSONString(oplogInsertJson)
		sql, err := openFile(flagCfg.InputFile, parser)

		if err != nil {
			return err
		} else {
			// fmt.Println("rootCmd sql is ->", sql)
			writeToFileActions(sql)

		}
		return nil
	},
}

func openOrCreateFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error opening/creating file -> %s\n err: %s", fileName, err)
	}
	return file, nil
}

func writeToFileActions(sql []string) {
	var file *os.File
	var err error
	file, err = openOrCreateFile(flagCfg.OutputFile)
	if err != nil {
		fmt.Println("Error creating file", err)
	}
	defer file.Close()

	_, err = file.WriteString(strings.Join(sql, ", ") + "\n")
	if err != nil {
		fmt.Printf("error writing to output file -> %v\n", err)
	}

}

func openFile(fileName string, p *Parser) ([]string, error) {
	// var input io.Reader
	file, err := os.Open(fileName)
	if err != nil {
		return []string{}, fmt.Errorf("error opening the file")
	}

	defer file.Close()

	dec := json.NewDecoder(file)

	t, err := dec.Token()
	if err != nil {
		return []string{}, fmt.Errorf("error with json input")
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return []string{}, fmt.Errorf("expected [ at start of JSON array")
	}

	sqlStatements := []string{}

	for dec.More() {
		var entry Oplog
		if err := dec.Decode(&entry); err != nil {
			return []string{}, fmt.Errorf("error decoding json into Oplog struct")

		} else {
			// fmt.Printf("%+v\n", entry)
			sql, err := p.getSqlStatements(entry)
			if err != nil {
				return []string{}, err

			} else {
				sqlStatements = append(sqlStatements, sql...)

			}
		}
	}

	t, err = dec.Token()
	if err != nil {
		return []string{}, err
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		return []string{}, fmt.Errorf("expected ] at the end of JSON array")
	}
	return sqlStatements, nil

}

func (p *Parser) getSqlStatements(oplog Oplog) ([]string, error) {
	sql, err := p.parseJsonStruct(oplog)
	if err != nil {
		return []string{}, fmt.Errorf("error parsing oplog struct -> %v", err)
	} else {
		return sql, nil
	}
}

func (p *Parser) parseJsonStruct(oplog Oplog) ([]string, error) {
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
					insertValues = append(insertValues, fmt.Sprintf("'%s'", p.idGenerator(16)))
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

func (p *Parser) updateSql(oplog Oplog) (string, error) {
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

	output += fmt.Sprintf("UPDATE %s SET %s WHERE %s;", oplog.Namespace, strings.Join(fieldsWithValues, ", "), strings.Join(whereColumnsPaired, " AND "))

	return output, nil
}

func deleteSql(oplog Oplog) (string, error) {
	output := ""
	whereColumnsPaired := []string{}

	whereColumnsPaired = appendedColumnsAndValues(whereColumnsPaired, oplog.Record)

	output += fmt.Sprintf("DELETE FROM %s WHERE %s;", oplog.Namespace, strings.Join(whereColumnsPaired, " AND "))
	return output, nil
}

func appendedColumnsAndValues(appendSlice []string, columnsMap map[string]interface{}) []string {
	for key, value := range columnsMap {
		key = strings.ToLower(key)
		switch v := value.(type) {
		case string:
			safeVal := strings.ReplaceAll(v, "'", "''")
			appendSlice = append(appendSlice, fmt.Sprintf("%s = '%s'", key, safeVal))
		case bool:
			appendSlice = append(appendSlice, fmt.Sprintf("%s = %t", key, v))
		case float64, int:
			appendSlice = append(appendSlice, fmt.Sprintf("%s = %v", key, v))
		default:
			appendSlice = append(appendSlice, fmt.Sprintf("%s = %v", key, v))
		}
	}
	return appendSlice
}

func nestedDocument(value any) bool {
	switch value.(type) {
	case string, int, bool, float64:
		return false
	case []interface{}:
		return true
	case interface{}:
		return true
	}
	return false
}

func randString(n int) string {

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func printToStdErr(err error) {
	fmt.Fprint(os.Stderr, err)
}
