package parser_test

import (
	"testing"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

func TestParser_GetSqlStatements(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		oplog   parser.Oplog
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser()
			got, gotErr := p.GenerateSql(tt.oplog)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("GetSqlStatements() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("GetSqlStatements() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("GetSqlStatements() = %v, want %v", got, tt.want)
			}
		})
	}
}
