package input

import (
	"context"
	"sync"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type SqlStatement struct {
	Sql        string
	IsBoundary bool
}

type Reader interface {
	Read(streamCtx context.Context, cfg *config.Config, p parser.Parser,
		oplogChan chan<- parser.Oplog, errChan chan<- errors.AppError, wg *sync.WaitGroup)
}
