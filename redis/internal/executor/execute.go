package executor

import (
	"fmt"
	"strconv"
	"strings"
)

type Store struct {
	Data map[string]string
}

func NewStore() *Store {
	return &Store{Data: map[string]string{}}
}

type command struct {
	command string
	key     string
	value   string
}

func (s *Store) Execute(input string) string {
	command := createCommand(input)
	switch strings.ToLower(command.command) {
	case "set":
		s.Data[command.key] = command.value
		return "OK"
	case "get":
		val, ok := s.Data[command.key]
		if !ok {
			return "(nil)"
		} else {
			return val
		}
	case "del":
		_, ok := s.Data[command.key]
		if !ok {
			return "(integer) 0"
		} else {
			delete(s.Data, command.key)
			return "(integer) 1"

		}
	case "incr":

		if command.value != "" {
			return "(error) ERR wrong number of arguments for 'incr' command"
		}

		val, ok := s.Data[command.key]
		if !ok {
			s.Data[command.key] = "1"
			return "(integer) 1"
		} else {
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return "(error) ERR value is not an integer or out of range"
			} else {
				finalVal := intVal + 1
				s.Data[command.key] = strconv.Itoa(finalVal)
				return fmt.Sprintf("(integer) %s", s.Data[command.key])
			}

		}
	case "incrby":
		if command.value == "" {
			return "(error) ERR wrong number of arguments for 'incrby' command"
		}

		val, ok := s.Data[command.key]
		if !ok {
			s.Data[command.key] = command.value
			return fmt.Sprintf("(integer) %v", s.Data[command.key])
		} else {

			intVal, _ := strconv.Atoi(val)
			intArg, err := strconv.Atoi(command.value)
			if err != nil {
				return "(error) ERR value is not an integer or out of range"
			} else {
				finalVal := intVal + intArg
				fmt.Println("finalVal is", finalVal)
				s.Data[command.key] = strconv.Itoa(finalVal)
				return fmt.Sprintf("(integer) %v", finalVal)
			}
		}

	}
	return ""
}

func createCommand(line string) *command {
	inputArgs := strings.Split(line, " ")

	if len(inputArgs) > 2 {
		return &command{command: inputArgs[0], key: inputArgs[1], value: inputArgs[2]}
	} else {
		return &command{command: inputArgs[0], key: inputArgs[1]}
	}
}
