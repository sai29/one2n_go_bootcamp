package executor

import (
	"strings"
)

type Store struct {
	Data          map[string]string
	Queued        []command
	inTransaction bool
}

func NewStore() *Store {
	return &Store{Data: map[string]string{}, inTransaction: false}
}

func (s *Store) Execute(command command) string {

	switch strings.ToLower(command.command) {
	case "set":
		return s.set(command)
	case "get":
		return s.get(command)
	case "del":
		return s.delete(command)
	case "incr":
		return s.increment(command)
	case "incrby":
		return s.incrementBy(command)
	case "multi":
		return s.multi()
	case "discard":
		return s.discard()
	case "exec":
		return s.exec()
	case "compact":
		return s.compact()
	}

	return ""
}
