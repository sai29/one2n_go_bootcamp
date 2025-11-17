package executor

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type command struct {
	command string
	key     string
	value   string
}

func CreateCommand(line string) command {
	inputArgs := strings.Split(line, " ")

	if len(inputArgs) > 2 {
		return command{command: inputArgs[0], key: inputArgs[1], value: inputArgs[2]}
	} else if len(inputArgs) == 2 {
		return command{command: inputArgs[0], key: inputArgs[1]}
	} else {
		return command{command: inputArgs[0]}
	}
}

func (s *Session) get(command command, store *Store) string {
	switch s.inTransaction {
	case true:
		s.Queued = append(s.Queued, command)
		return "QUEUED"
	default:
		val, ok := store.Data[command.key]
		if !ok {
			return "(nil)"
		} else {
			return val
		}
	}
}

func (s *Session) set(command command, store *Store) string {
	switch s.inTransaction {
	case true:
		s.Queued = append(s.Queued, command)
		return "QUEUED"
	default:
		store.Data[command.key] = command.value
		return "OK"
	}
}

func (s *Session) delete(command command, store *Store) string {
	switch s.inTransaction {
	case true:
		s.Queued = append(s.Queued, command)
		return "QUEUED"
	default:
		_, ok := store.Data[command.key]
		if !ok {
			return "(integer) 0"
		} else {
			delete(store.Data, command.key)
			return "(integer) 1"
		}
	}
}

func (s *Session) increment(command command, store *Store) string {
	switch s.inTransaction {
	case true:
		s.Queued = append(s.Queued, command)
		return "QUEUED"
	default:
		if command.value != "" {
			return "(error) ERR wrong number of arguments for 'incr' command"
		}

		val, ok := store.Data[command.key]
		if !ok {
			store.Data[command.key] = "1"
			return "(integer) 1"
		} else {
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return "(error) ERR value is not an integer or out of range"
			} else {
				finalVal := intVal + 1
				store.Data[command.key] = strconv.Itoa(finalVal)
				return fmt.Sprintf("(integer) %s", store.Data[command.key])
			}
		}
	}
}

func (s *Session) incrementBy(command command, store *Store) string {
	switch s.inTransaction {
	case true:
		s.Queued = append(s.Queued, command)
		return "QUEUED"
	default:
		if command.value == "" {
			return "(error) ERR wrong number of arguments for 'incrby' command"
		}

		val, ok := store.Data[command.key]
		if !ok {
			store.Data[command.key] = command.value
			return fmt.Sprintf("(integer) %v", store.Data[command.key])
		} else {

			intVal, _ := strconv.Atoi(val)
			intArg, err := strconv.Atoi(command.value)
			if err != nil {
				return "(error) ERR value is not an integer or out of range"
			} else {
				finalVal := intVal + intArg
				store.Data[command.key] = strconv.Itoa(finalVal)
				return fmt.Sprintf("(integer) %v", finalVal)
			}
		}
	}
}

func (s *Session) multi() string {
	switch s.inTransaction {
	case true:
		return "(error) ERR Command not allowed inside a transaction"
	default:
		s.inTransaction = true
		return "OK"
	}
}

func (s *Session) discard() string {
	s.inTransaction = false
	s.Queued = nil
	return "OK"
}

func (s *Session) exec(store *Store) string {
	var b strings.Builder
	s.inTransaction = false
	for index, command := range s.Queued {
		fmt.Fprintf(&b, "%v) %s", index+1, s.Execute(command, store))
		if index != len(s.Queued)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (s *Session) compact(store *Store) string {
	var b strings.Builder
	var keys []string

	for key := range store.Data {
		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return "(nil)"
	}

	slices.Sort(keys)
	for index, key := range keys {
		fmt.Fprintf(&b, "SET %s %s", key, store.Data[key])
		if index != len(keys)-1 {
			b.WriteString("\n")
		}

	}

	return b.String()
}

func (s *Session) selectDb(cmd command) string {
	idx, err := strconv.Atoi(cmd.key)
	if err != nil {
		return "(error) ERR value is not an integer or out of range"
	}
	if idx < 0 || idx >= 16 {
		return "(error) ERR DB index is out of range"
	}
	s.CurrentDbIndex = idx
	return "OK"
}
