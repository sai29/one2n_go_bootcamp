package output

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
)

type PostgresWriter struct {
	db *sql.DB
}

func NewPostgresWriter(uri string) *PostgresWriter {
	db, err := sql.Open("postgres", "postgres://oplog_replica:password@localhost/oplog_replica?sslmode=disable")
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	duration, err := time.ParseDuration("15m")
	if err != nil {
		panic(err)
	}

	db.SetConnMaxIdleTime(duration)

	err = db.Ping()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Connected to and pinged DB")
	}

	return &PostgresWriter{db: db}
}

func (pw *PostgresWriter) Write(ctx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- error) {
	// fmt.Println("Received sql to write to postgres", sql)
	defer pw.db.Close()

	tx, err := pw.db.Begin()
	if err != nil {
		errChan <- fmt.Errorf("failed to begin transaction: %w", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			tx.Rollback()
			errChan <- ctx.Err()
			return
		case msg, ok := <-sqlChan:
			fmt.Printf("Sql sent is -> %s", msg.Sql)
			if !ok {
				if err := tx.Commit(); err != nil {
					errChan <- fmt.Errorf("commit failed: %w", err)
				}
				return
			}

			if msg.IsBoundary {
				if err := tx.Commit(); err != nil {
					errChan <- fmt.Errorf("commit failed: %w", err)
				}
				tx, err = pw.db.Begin()
				if err != nil {
					errChan <- fmt.Errorf("begin transaction failed: %w", err)
					return
				}
				continue
			}

			if _, err := tx.Exec(msg.Sql); err != nil {
				tx.Rollback()
				errChan <- fmt.Errorf("exec failed, rolled back: %w", err)
				tx, err = pw.db.Begin()
				if err != nil {
					errChan <- fmt.Errorf("restarting tx failed: %w", err)
					return
				}
			}
		}
	}
}
