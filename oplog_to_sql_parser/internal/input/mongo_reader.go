package input

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/config"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoReader struct {
	uri string
}

func NewMongoReader(uri string) *MongoReader {
	return &MongoReader{uri: uri}
}

func (mr *MongoReader) Read(ctx context.Context, config *config.Config, p *parser.Parser) ([]string, error) {
	// connString := "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"))
	if err != nil {
		return []string{}, err
	}

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			fmt.Println("disconnect from mongo db")
		}
	}()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		fmt.Println("error with ping to mongo database")
		fmt.Println("ping err ->", err)
		return nil, err
	}

	dbs, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list databases failed: %w", err)
	}

	fmt.Println("Databases:", dbs)
	startTs, err := ReadOplogLatest(client)
	if err != nil {

		return nil, fmt.Errorf("failed to get latest ts from oplog: %w", err)
	}

	cursor, err := OpenTailableCursor(ctx, client, startTs)
	if err != nil {
		return nil, fmt.Errorf("failed to open tailable cursor: %w", err)
	}

	defer cursor.Close(ctx)

	sql, err := ProcessOplogs(ctx, cursor, p, config)
	fmt.Println("MongoDBConnActions is ->", sql, err)
	if err != nil {
		fmt.Println("Are we entering here? error land")
		return nil, fmt.Errorf("oplog processing failed: %w", err)
	}

	return sql, nil
}

func ReadOplogLatest(client *mongo.Client) (bson.RawValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	oplog := client.Database("local").Collection("oplog.rs")

	opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	var doc struct {
		Ts bson.RawValue `bson:"ts"`
	}

	err := oplog.FindOne(ctx, bson.M{}, opts).Decode(&doc)
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("find latest oplog entry failed: %w", err)
	}

	fmt.Println("Latest oplog entry:", doc.Ts)
	return doc.Ts, nil
}

func OpenTailableCursor(ctx context.Context, client *mongo.Client, startTs bson.RawValue) (*mongo.Cursor, error) {

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

func ProcessOplogs(ctx context.Context, cursor *mongo.Cursor, p *parser.Parser, config *config.Config) ([]string, error) {

	var allSql []string
	fmt.Println("Entering here")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled, stopping oplog processing")
			return allSql, nil
		default:
		}
		if cursor.TryNext(context.TODO()) {

			var data bson.M
			if err := cursor.Decode(&data); err != nil {
				fmt.Println("failed to decode oplog entry:", err)
				panic(err)
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				panic(err)
			}

			var entry parser.Oplog
			err = json.Unmarshal(jsonData, &entry)
			if err != nil {
				return []string{}, err
			}

			switch entry.Op {
			case "i", "u", "d":

				sql, err := p.ParseJsonStruct(entry)
				if err != nil {
					fmt.Println("error parsing oplog to SQL:", err)
					continue
				}

				allSql = append(allSql, sql...)

			default:
				continue
			}
		}

		if err := cursor.Err(); err != nil {
			fmt.Printf("error from cursor returning...err -> %s", err)
			return allSql, fmt.Errorf("cursor error: %w", err)
		}

		if cursor.ID() == 0 {
			fmt.Println("Cursor closed by server, existing loop")
			break
		}

	}
	return allSql, nil

}
