package executor

import "strings"

var store = map[string]string{}

func Execute(input string) string {
	inputArgs := strings.Split(input, " ")

	switch inputArgs[0] {
	case "SET":
		store[inputArgs[1]] = inputArgs[2]
		return "OK"
	case "GET":
		val, ok := store[inputArgs[1]]
		if !ok {
			return "(nil)"
		} else {
			return val
		}
	case "DEL":
		_, ok := store[inputArgs[1]]
		if !ok {
			return "(integer) 0"
		} else {
			delete(store, inputArgs[1])
			return "(integer) 1"

		}
	}
	return ""
}
