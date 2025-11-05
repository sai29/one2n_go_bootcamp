package bookmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/errors"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/logx"
	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type Bookmark struct {
	LastTS struct {
		T int `json:"T"`
		I int `json:"I"`
	} `json:"last_ts"`
	LastNamespace string `json:"last_namespace"`
}

func Load(path string) (parser.Bookmark, error) {
	tsFile, err := os.OpenFile("bookmark.json", os.O_RDONLY, 0644)
	if err != nil {
		return parser.Bookmark{}, err
	}

	defer tsFile.Close()

	tsDec := json.NewDecoder(tsFile)
	var bk parser.Bookmark

	err = tsDec.Decode(&bk)
	if err != nil {
		return parser.Bookmark{}, nil
	}
	logx.Info("Bookmark -> %+v", bk)

	return bk, nil

}

func BookmarkWorker(ctx context.Context, bookmarkChan chan map[string]int, errChan chan errors.AppError) {
	for {
		select {
		case <-ctx.Done():
			return
		case bk, ok := <-bookmarkChan:
			if !ok {
				logx.Info("Bookmark worker returning after bookmarkChan closed.")
				return
			}

			if err := SaveBookmark("bookmark.json", bk["currentT"], bk["currentI"]); err != nil {
				logx.Error("error saving bookmark timestamp -> %s", err)
				errors.SendWarn(errChan, fmt.Errorf("error saving bookmark timestamp -> %s", err))
			} else {
				logx.Info("Saved bookmark successfuly -> %v", bk)
			}

		}
	}

}

func SaveBookmark(path string, t int, i int) error {
	var bookmark parser.Bookmark

	bookmark.LastTS.I = i
	bookmark.LastTS.T = t

	data, err := json.MarshalIndent(bookmark, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal bookmark: %s", err)
	}

	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to wrtie temp file: %s", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("faile to rename temp file: %s", err)
	}
	return nil
}

func OplogAfterBookmark(bkTimeStamp, bkIncrement,
	currentTimestamp, currentIncrement int) bool {
	return bkTimeStamp == 0 ||
		(currentTimestamp > bkTimeStamp) ||
		(currentTimestamp == bkTimeStamp &&
			currentIncrement > bkIncrement)
}
