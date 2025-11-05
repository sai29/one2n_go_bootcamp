package input

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/bookmark"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var systemNamespace = []string{"admin", "local", "config"}

type MongoReader struct {
	uri string
}

func NewMongoReader(uri string) *MongoReader {
	return &MongoReader{uri: uri}
}

func (mr *MongoReader) Read(ctx context.Context, config *config.Config,
	p parser.Parser, oplogChan chan<- parser.Oplog,
	errChan chan<- errors.AppError, wg *sync.WaitGroup) {

	// connString := "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
	defer close(oplogChan)

	clientOpts := options.Client().
		ApplyURI(mr.uri).
		SetServerSelectionTimeout(2 * time.Second).
		SetConnectTimeout(2 * time.Second)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("failed to create Mongo client: %w", err))
	}

	defer func() {
		discCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := client.Disconnect(discCtx); err != nil {
			logx.Info("warning: disconnect timeout or failure -> %v", err)
		}
	}()

	logx.Info("Trying to ping MongoDB...")

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		errors.SendFatal(errChan, fmt.Errorf("error with ping to mongodb -> %v", err))

		go func() {
			_ = client.Disconnect(context.Background())
		}()

		return
	}

	bk, err := bookmark.Load("bookmark.json")
	if err != nil {
		if err != io.EOF {
			errors.SendWarn(errChan, fmt.Errorf("couldn't decode timestamp json into bookmark struct: %s", err))
		} else {
			logx.Info("Empty file")
		}
	}

	var startTs bson.Timestamp

	if bk.LastTS.T == 0 {
		startTs, err = ReadOplogLatest(client)
		if err != nil {
			errors.SendWarn(errChan, fmt.Errorf("error getting latest Ts from oplog -> %v", err))
		}

	} else {
		startTs.T, startTs.I = uint32(bk.LastTS.T), uint32(bk.LastTS.I)
	}

	cursor, err := OpenTailableCursor(ctx, client, startTs)
	if err != nil {
		errors.SendFatal(errChan, fmt.Errorf("failed to open tailable cursor: %w", err))
	}

	defer cursor.Close(ctx)

	err = ProcessOplogs(ctx, cursor, p, config, oplogChan, bk)
	if err != nil {
		errors.SendWarn(errChan, fmt.Errorf("oplog processing failed: %w", err))
	}

}

func ReadOplogLatest(client *mongo.Client) (bson.Timestamp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	oplog := client.Database("local").Collection("oplog.rs")

	opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	var doc struct {
		Ts bson.RawValue `bson:"ts"`
	}

	err := oplog.FindOne(ctx, bson.M{}, opts).Decode(&doc)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("find latest oplog entry failed: %w", err)
	}

	var timeStamp bson.Timestamp

	timeStamp.T, timeStamp.I = doc.Ts.Timestamp()

	logx.Info("Latest oplog entry -> %v", doc.Ts)
	return timeStamp, nil
}

func OpenTailableCursor(ctx context.Context, client *mongo.Client, startTs bson.Timestamp) (*mongo.Cursor, error) {

	oplog := client.Database("local").Collection("oplog.rs")

	filter := bson.M{"ts": bson.M{"$gte": startTs}}

	findOpts := options.Find()
	findOpts.SetCursorType(options.TailableAwait)
	findOpts.SetNoCursorTimeout(true)

	cursor, err := oplog.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open tailable cursor: %w", err)
	}

	logx.Info("Tailable cursor opened from ts -> %v", startTs)
	return cursor, nil

}

func ProcessOplogs(ctx context.Context, cursor *mongo.Cursor, p parser.Parser, config *config.Config,
	oplogChan chan<- parser.Oplog, savedBookmark parser.Bookmark) error {

	for {
		select {
		case <-ctx.Done():
			logx.Info("Context cancelled, stopping oplog processing")
			return nil
		default:
		}
		if cursor.TryNext(ctx) {

			var data bson.M
			if err := cursor.Decode(&data); err != nil {
				logx.Info("failed to decode oplog entry -> %v", err)
				continue
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				logx.Info("failed to marshal json -> %v", err)
				continue
			}

			logx.Info("Json data ->  %v", string(jsonData))

			var entry parser.Oplog
			err = json.Unmarshal(jsonData, &entry)
			if err != nil {
				logx.Info("failed to unmarshal json -> %v", err)
			} else {
				switch entry.Op {
				case "i", "u", "d":

					namespaceCollection := strings.Split(entry.Namespace, ".")[0]

					if !slices.Contains(systemNamespace, namespaceCollection) {

						logx.Info("entry is %+v", entry.Record)
						oplogChan <- entry
					}
				default:
					continue
				}

			}
		}

		if err := cursor.Err(); err != nil {
			logx.Info("Closing down cursor...")
			return nil
		}

		if cursor.ID() == 0 {
			logx.Info("Cursor closed by server, existing loop")
			break
		}

	}
	return nil

}
