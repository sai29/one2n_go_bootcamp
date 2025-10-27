package dispatcher

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type Dispatcher struct {
	dbWorkers map[string]*dbWorker
}

type dbWorker struct {
	name            string
	dbChan          chan parser.Oplog
	sqlChan         chan input.SqlStatement
	collectionChans map[string]chan parser.Oplog
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{dbWorkers: make(map[string]*dbWorker)}
}

func (d *Dispatcher) Dispatch(ctx context.Context, oplog <-chan parser.Oplog, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-oplog:
			if !ok {
				return
			}
			nameSpace := strings.Split(op.Namespace, ".")

			dbName := nameSpace[0]

			if _, exists := d.dbWorkers[dbName]; !exists {

				worker := &dbWorker{
					name:            dbName,
					sqlChan:         make(chan input.SqlStatement, 100),
					dbChan:          make(chan parser.Oplog, 100),
					collectionChans: make(map[string]chan parser.Oplog),
				}

				d.dbWorkers[dbName] = worker

				wg.Add(1)
				fmt.Println("Calling db worker")
				go worker.processDB(ctx, wg)

				wg.Add(1)
				fmt.Println("calling sql executor")
				go sqlExecutor(ctx, worker.sqlChan, wg)

			}

			d.dbWorkers[dbName].dbChan <- op

		}
	}
}

func (db *dbWorker) processDB(ctx context.Context, wg *sync.WaitGroup) {
	fmt.Println("inside db worker")
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case oplog, ok := <-db.dbChan:
			if !ok {
				return
			}
			nameSpace := strings.Split(oplog.Namespace, ".")
			collection := nameSpace[1]

			if _, exists := db.collectionChans[collection]; !exists {
				collectionChan := make(chan parser.Oplog, 100)
				db.collectionChans[collection] = collectionChan

				wg.Add(1)
				fmt.Println("Calling collections worker")
				go collectionWorker(ctx, collectionChan, wg)
			}
			db.collectionChans[collection] <- oplog
		}
	}

}

func sqlExecutor(ctx context.Context, sqlChan <-chan input.SqlStatement, wg *sync.WaitGroup) {
	fmt.Println("inside sqlExector")

	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Cancelling from sql executor")
			return
		case stmt, ok := <-sqlChan:
			if !ok {
				return
			}
			fmt.Println("Statement is", stmt)
		}
	}

}

func collectionWorker(ctx context.Context, oplog chan parser.Oplog, wg *sync.WaitGroup) {
	defer wg.Done()
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
			fmt.Printf("Processing oplog -> %+v\n", oplog.TimeStamp)
			// fmt.Println("Sleeping")
			// time.Sleep(100 * time.Millisecond)

		}
	}
}
