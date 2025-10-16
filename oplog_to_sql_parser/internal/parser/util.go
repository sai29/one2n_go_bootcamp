package parser

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func inferSQLType(key any, value any) string {
	switch value.(type) {
	case string:
		if key == "_id" {
			return "_id VARCHAR(255) PRIMARY KEY"
		}
		return fmt.Sprintf("%s VARCHAR(255)", key)
	case bool:
		return fmt.Sprintf("%s BOOLEAN", key)
	case float64, int:
		return fmt.Sprintf("%s FLOAT", key)
	default:
		return ""
	}
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

func PrintToStdErr(err error) {
	fmt.Fprint(os.Stderr, err)
}
