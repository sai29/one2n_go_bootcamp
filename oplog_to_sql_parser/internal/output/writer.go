package output

import (
	"context"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
)

type Writer interface {
	Write(streamCtx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- error)
}
