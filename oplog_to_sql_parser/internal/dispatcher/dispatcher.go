package dispatcher

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/bookmark"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type Dispatcher struct {
	dbWorkers  map[string]*dbWorker
	dbWorkerWg *sync.WaitGroup
	parser     parser.Parser
}

type dbWorker struct {
	name            string
	dbChan          chan parser.Oplog
	collectionChans map[string]chan parser.Oplog
	parser          parser.Parser
	collectionWg    *sync.WaitGroup
	bookmarkChan    chan map[string]int
}

func NewDispatcher(p parser.Parser) *Dispatcher {
	return &Dispatcher{dbWorkers: make(map[string]*dbWorker), parser: p, dbWorkerWg: &sync.WaitGroup{}}
}

func (d *Dispatcher) Dispatch(ctx context.Context, oplog <-chan parser.Oplog,
	bookmarkChan chan map[string]int, sqlChan chan input.SqlStatement, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-oplog:
			if !ok {
				fmt.Println("Oplog chan closed, closing all dbChans..")
				for _, worker := range d.dbWorkers {
					close(worker.dbChan)
				}
				d.dbWorkerWg.Wait()
				close(sqlChan)
				close(d.parser.GetParserReqChan())

				return
			}
			nameSpace := strings.Split(op.Namespace, ".")

			dbName := nameSpace[0]
			worker, exists := d.dbWorkers[dbName]

			if !exists {

				worker = &dbWorker{
					name:            dbName,
					dbChan:          make(chan parser.Oplog, 100),
					collectionChans: make(map[string]chan parser.Oplog),
					parser:          d.parser,
					collectionWg:    &sync.WaitGroup{},
					bookmarkChan:    bookmarkChan,
				}

				d.dbWorkers[dbName] = worker

				wg.Add(1)
				d.dbWorkerWg.Add(1)
				go func() {
					fmt.Println("Calling db worker")
					defer d.dbWorkerWg.Done()
					worker.processDB(ctx, errChan, sqlChan, wg)
				}()
			}

			select {
			case <-ctx.Done():
				return
			case worker.dbChan <- op:
			}

		}
	}
}

func (dbW *dbWorker) processDB(ctx context.Context, errChan chan<- error,
	sqlChan chan input.SqlStatement, wg *sync.WaitGroup) {
	fmt.Println("inside db worker")
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case oplog, ok := <-dbW.dbChan:
			if !ok {
				fmt.Println("db chan closed, closing all connection chans..")
				for _, ch := range dbW.collectionChans {
					close(ch)
				}

				dbW.collectionWg.Wait()
				return
			}
			nameSpace := strings.Split(oplog.Namespace, ".")
			collection := nameSpace[1]

			if _, exists := dbW.collectionChans[collection]; !exists {
				collectionChan := make(chan parser.Oplog, 100)
				dbW.collectionChans[collection] = collectionChan

				dbW.collectionWg.Add(1)
				wg.Add(1)
				fmt.Println("Calling collections worker")
				go dbW.collectionWorker(ctx, collectionChan, sqlChan, errChan, wg)
			}
			dbW.collectionChans[collection] <- oplog
		}
	}

}

func (dbW *dbWorker) collectionWorker(ctx context.Context, oplog chan parser.Oplog,
	sqlChan chan input.SqlStatement, errChan chan<- error, wg *sync.WaitGroup) {

	defer wg.Done()
	defer dbW.collectionWg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Cancelling from collection worker")
			return
		case oplog, ok := <-oplog:
			if !ok {
				return
			}
			nameSpace := strings.Split(oplog.Namespace, ".")

			collection := nameSpace[1]

			fmt.Println("Inside collection worker of collection", collection)
			// fmt.Printf("Processing oplog -> %+v\n", oplog.TimeStamp)

			bk, err := bookmark.Load("bookmark.json")
			if err != nil {
				if err != io.EOF {
					errChan <- fmt.Errorf("couldn't decode timestamp json into bookmark struct: %s", err)
				} else {
					fmt.Println("Empty file")
				}
			}

			bkTimeStamp, bkIncrement := bk.LastTS.T, bk.LastTS.I

			currentT := int(oplog.TimeStamp["T"].(float64))
			currentI := int(oplog.TimeStamp["I"].(float64))

			if bookmark.OplogAfterBookmark(bkTimeStamp, bkIncrement, currentT, currentI) {

				respChan := make(chan parser.ParserResp)

				select {
				case <-ctx.Done():
					return
				case dbW.parser.GetParserReqChan() <- parser.ParserRequest{Oplog: oplog, RespChan: respChan}:
				}

				select {
				case <-ctx.Done():
					return
				case resp, ok := <-respChan:
					if !ok {
						return
					}
					if resp.Err != nil {
						errChan <- fmt.Errorf("error from GetSqlStatements -> %v", err)
					} else {
						for _, stmt := range resp.Sql {
							sqlChan <- input.SqlStatement{Sql: stmt, IsBoundary: false}
						}
						sqlChan <- input.SqlStatement{IsBoundary: true}

						select {
						case <-ctx.Done():
							return
						case dbW.bookmarkChan <- map[string]int{"currentI": currentI, "currentT": currentT}:
						}
					}
				}
			} else {
				fmt.Println("Skipping oplog because of timestamp")
			}

		}
	}
}
