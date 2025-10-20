package bookmark

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

type Bookmark struct {
	LastTS struct {
		T int `json:"T"`
		I int `json:"I"`
	} `json:"last_ts"`
	LastNamespace string `json:"last_namespace"`
}

func SaveBookmark(path string, t int) error {
	var bookmark parser.Bookmark

	bookmark.LastTS.I = 0
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
