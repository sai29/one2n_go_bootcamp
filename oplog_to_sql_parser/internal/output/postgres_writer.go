package output

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/input"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
)

type PostgresWriter struct {
	db *sql.DB
}

func NewPostgresWriter(uri string) (*PostgresWriter, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	duration, err := time.ParseDuration("15m")
	if err != nil {
		db.Close()
		return nil, err
		// panic(err)
	}

	db.SetConnMaxIdleTime(duration)

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
		// panic(err)
	} else {
		logx.Info("Connected to and pinged Postgres DB")
	}

	return &PostgresWriter{db: db}, nil
}

func (pw *PostgresWriter) Write(ctx context.Context, sqlChan <-chan input.SqlStatement, errChan chan<- errors.AppError) {
	defer pw.db.Close()

	tx, err := pw.db.Begin()
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("failed to begin transaction: %w", err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			tx.Rollback()
			return
		case msg, ok := <-sqlChan:
			logx.Info("Sql sent is -> %s", msg.Sql)
			if !ok {
				if err := tx.Commit(); err != nil {
					errors.SendWarn(errChan, fmt.Errorf("commit failed: %w", err))
				}
				return
			}

			if msg.IsBoundary {
				if err := tx.Commit(); err != nil {
					errors.SendWarn(errChan, fmt.Errorf("commit failed: %w", err))
				}
				tx, err = pw.db.Begin()
				if err != nil {
					errors.SendWarn(errChan, fmt.Errorf("begin transaction failed: %w", err))
					return
				}
				continue
			}

			if _, err := tx.Exec(msg.Sql); err != nil {
				tx.Rollback()
				errors.SendWarn(errChan, fmt.Errorf("exec failed, rolled back: %w", err))
				tx, err = pw.db.Begin()
				if err != nil {
					errors.SendFatal(errChan, fmt.Errorf("restarting tx failed: %w", err))
					return
				}
			}
		}
	}
}
