package parser

import (
	"fmt"
	"strings"
)

func deleteSql(oplog Oplog) (string, error) {
	output := ""
	whereColumnsPaired := []string{}

	whereColumnsPaired = appendedColumnsAndValues(whereColumnsPaired, oplog.Record)

	output += fmt.Sprintf("DELETE FROM %s WHERE %s;", oplog.Namespace, strings.Join(whereColumnsPaired, " AND "))
	return output, nil
}
