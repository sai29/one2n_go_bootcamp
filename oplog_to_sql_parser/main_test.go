package main

import (
	"testing"
)

func Test_updateSql(t *testing.T) {
	type args struct {
		oplog Oplog
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := updateSql(tt.args.oplog)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("updateSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_deleteSql(t *testing.T) {
	type args struct {
		oplog Oplog
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deleteSql(tt.args.oplog)
			if (err != nil) != tt.wantErr {
				t.Errorf("deleteSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("deleteSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getQualifiedTableName(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getQualifiedTableName(tt.args.tableName); got != tt.want {
				t.Errorf("getQualifiedTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_decodeJSONString(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		jsonOplog []string
		want      []string
		wantErr   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser()
			got, gotErr := p.decodeJSONString(tt.jsonOplog)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("decodeJSONString() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("decodeJSONString() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("decodeJSONString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_insertSql(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		oplog   Oplog
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser()
			got, gotErr := p.insertSql(tt.oplog)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("insertSql() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("insertSql() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("insertSql() = %v, want %v", got, tt.want)
			}
		})
	}
}
