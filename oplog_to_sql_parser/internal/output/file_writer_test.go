package output_test

import (
	"testing"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/output"
)

func TestFileWriter_Write(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		uri string
		// Named input parameters for target function.
		sql     []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fr := output.NewFileWriter(tt.uri)
			gotErr := fr.Write(tt.sql)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Write() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Write() succeeded unexpectedly")
			}
		})
	}
}
