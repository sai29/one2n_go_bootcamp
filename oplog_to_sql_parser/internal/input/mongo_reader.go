package input

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/bookmark"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
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

func (mr *MongoReader) Read(ctx context.Context, config *config.Config, p parser.Parser,
	sqlChan chan<- SqlStatement, errChan chan<- error) {
	// connString := "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"

	defer close(sqlChan)
	defer close(errChan)
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"))
	if err != nil {
		errChan <- fmt.Errorf("error connections to mongo client -> %v", err)
	}

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			errChan <- fmt.Errorf("disconnect from monogdb -> %v", err)
		}
	}()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		fmt.Println("error with ping to mongo database")
		errChan <- fmt.Errorf("ping err -> %v", err)
	}

	// dbs, err := client.ListDatabaseNames(ctx, bson.M{})
	// if err != nil {
	// 	errChan <- fmt.Errorf("list databases failed: %v", err)
	// }
	// fmt.Println("Databases:", dbs)

	tsFile, err := os.OpenFile("bookmark.json", os.O_RDONLY, 0644)
	if err != nil {
		errChan <- fmt.Errorf("error opening/creating file -> %s\n err: %s", "bookmark.json", err)
	}

	defer tsFile.Close()

	tsDec := json.NewDecoder(tsFile)
	var bk parser.Bookmark

	err = tsDec.Decode(&bk)
	fmt.Printf("Bookmark is %+v\n", bk)

	if err != nil {
		if err != io.EOF {
			errChan <- fmt.Errorf("couldn't decode timestamp json into bookmark struct: %s", err)
		} else {
			fmt.Println("Empty file")
		}
	}

	var startTs bson.Timestamp

	if bk.LastTS.T == 0 {
		startTs, err = ReadOplogLatest(client)
		if err != nil {
			errChan <- fmt.Errorf("error getting latest Ts from oplog -> %v", err)
		}

	} else {
		startTs.T, startTs.I = uint32(bk.LastTS.T), uint32(bk.LastTS.I)
	}

	cursor, err := OpenTailableCursor(ctx, client, startTs)
	if err != nil {
		errChan <- fmt.Errorf("failed to open tailable cursor: %w", err)
	}

	defer cursor.Close(ctx)

	err = ProcessOplogs(ctx, cursor, p, config, sqlChan, bk)
	if err != nil {
		errChan <- fmt.Errorf("oplog processing failed: %w", err)
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

	fmt.Println("Latest oplog entry:", doc.Ts)
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

	fmt.Println("Tailable cursor opened from ts:", startTs)
	return cursor, nil

}

func ProcessOplogs(ctx context.Context, cursor *mongo.Cursor, p parser.Parser, config *config.Config,
	sqlChan chan<- SqlStatement, savedBookmark parser.Bookmark) error {

	fmt.Println("Entering here")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled, stopping oplog processing")
			return nil
		default:
		}
		if cursor.TryNext(ctx) {

			var data bson.M
			if err := cursor.Decode(&data); err != nil {
				fmt.Println("failed to decode oplog entry:", err)
				continue
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				fmt.Println("failed to marshal json", err)
				continue
			}

			// fmt.Println("Json data is", string(jsonData))

			var entry parser.Oplog
			err = json.Unmarshal(jsonData, &entry)
			if err != nil {
				fmt.Println("failed to unmarshal json", err)
			}

			// fmt.Println("saved bookmark is ->", savedBookmark)

			currentTimestamp := int(entry.TimeStamp["T"].(float64))
			currentIncrement := int(entry.TimeStamp["I"].(float64))

			// fmt.Println("Current time stamp is -> ", currentTimestamp)

			if savedBookmark.LastTS.T == 0 ||
				(currentTimestamp > savedBookmark.LastTS.T) ||
				(currentTimestamp == savedBookmark.LastTS.T && currentIncrement > savedBookmark.LastTS.I) {

				switch entry.Op {
				case "i", "u", "d":

					namespaceCollection := strings.Split(entry.Namespace, ".")[0]
					if !slices.Contains(systemNamespace, namespaceCollection) {

						// fmt.Printf("entry is %+v", entry)

						sql, err := p.GenerateSql(entry)
						if err != nil {
							fmt.Println("error parsing oplog to SQL:", err)
							continue
						}

						for _, stmt := range sql {
							sqlChan <- SqlStatement{Sql: stmt, IsBoundary: false}
						}

						sqlChan <- SqlStatement{IsBoundary: true}

						if err := bookmark.SaveBookmark("bookmark.json", currentTimestamp, currentIncrement); err != nil {
							fmt.Println("error saving bookmark timestamp", err)
							return fmt.Errorf("error saving bookmark timestamp -> %s", err)
						} else {
							fmt.Println("saved bookmark successfully ->", currentTimestamp)
						}

					}
				default:
					continue
				}
			}
		}

		if err := cursor.Err(); err != nil {
			fmt.Println("Closing down cursor...")
			return nil
		}

		if cursor.ID() == 0 {
			fmt.Println("Cursor closed by server, existing loop")
			break
		}

	}
	return nil

}
