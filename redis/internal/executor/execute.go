package executor

import (
	"strings"
)

type DbMaster struct {
	Dbs map[int]*Store
}

type Store struct {
	Data map[string]string
}

type Session struct {
	Queued         []command
	inTransaction  bool
	CurrentDbIndex int
}

func NewDbMaster() *DbMaster {
	m := &DbMaster{Dbs: make(map[int]*Store)}
	for i := 0; i < 16; i++ {
		m.Dbs[i] = NewStore()
	}
	return m
}

func NewStore() *Store {
	return &Store{Data: map[string]string{}}
}

func NewSession() *Session {
	return &Session{inTransaction: false, CurrentDbIndex: 0}
}

func (s *Session) Execute(command command, store *Store) string {

	switch strings.ToLower(command.command) {
	case "set":
		return s.set(command, store)
	case "get":
		return s.get(command, store)
	case "del":
		return s.delete(command, store)
	case "incr":
		return s.increment(command, store)
	case "incrby":
		return s.incrementBy(command, store)
	case "multi":
		return s.multi()
	case "discard":
		return s.discard()
	case "exec":
		return s.exec(store)
	case "compact":
		return s.compact(store)
	case "select":
		return s.selectDb(command)
	}

	return ""
}
