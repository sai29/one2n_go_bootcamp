package parser_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sai29/one2n_go_bootcamp/oplog_to_sql_parser/internal/parser"
)

func fakeIDGenerator(n int) string {
	return "64798c213f273a7ca2cf516e"
}

func TestParser_GetSqlStatements(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		oplog   parser.Oplog
		want    []string
		wantErr bool
	}{
		{
			name:    "Empty struct",
			oplog:   parser.Oplog{},
			want:    []string{},
			wantErr: true,
		},
		{
			name:    "Insert Oplog - Create table and insert stmt and nested documents should create new tables",
			oplog:   parser.Oplog{Op: "i", Namespace: "employee.employees", Record: map[string]interface{}{"_id": "64798c213f273a7ca2cf516c", "address": []interface{}{map[string]interface{}{"line1": "32550 Port Gatewaytown", "zip": "18399"}, map[string]interface{}{"line1": "3840 Cornermouth", "zip": "83941"}}, "age": 35, "name": "Raymond Monahan", "phone": map[string]interface{}{"personal": "8764255212", "work": "2762135091"}, "position": "Engineer", "salary": 3767.925634753098}, UpdateColumns: map[string]interface{}{"_id": "64798c213f273a7ca2cf516c"}, TableCreated: false, TimeStamp: map[string]interface{}{"I": 5, "T": 1.685687329e+09}},
			want:    []string{"CREATE SCHEMA employee;", "CREATE TABLE IF NOT EXISTS employee.employees (_id VARCHAR(255) PRIMARY KEY, age FLOAT, name VARCHAR(255), position VARCHAR(255), salary FLOAT);", "INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf516c', 35, 'Raymond Monahan', 'Engineer', 3767.925634753098);", "CREATE TABLE IF NOT EXISTS employee.employees_address (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516c', '32550 Port Gatewaytown', '18399');", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516c', '3840 Cornermouth', '83941');", "CREATE TABLE IF NOT EXISTS employee.employees_phone (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));", "INSERT INTO employee.employees_phone (_id, employees__id, personal, work) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516c', '8764255212', '2762135091');"},
			wantErr: false,
		},
		{
			name:    "Update Oplog - Create update stmt",
			oplog:   parser.Oplog{Op: "u", Namespace: "employee.employees", Record: map[string]interface{}{"$v": 2, "diff": map[string]interface{}{"u": map[string]interface{}{"Age": 23}}}, UpdateColumns: map[string]interface{}{"_id": "64798c213f273a7ca2cf5171"}, TableCreated: false, TimeStamp: map[string]interface{}{"I": 12, "T": 1.685687337e+09}},
			want:    []string{"UPDATE employee.employees SET age = 23 WHERE _id = '64798c213f273a7ca2cf5171';"},
			wantErr: false,
		},
		{
			name:    "Delete Oplog",
			oplog:   parser.Oplog{Op: "d", Namespace: "employee.employees", Record: map[string]interface{}{"_id": "64798c213f273a7ca2cf516c"}, UpdateColumns: map[string]interface{}(nil), TableCreated: false, TimeStamp: map[string]interface{}{"I": 6, "T": 1.685687331e+09}},
			want:    []string{"DELETE FROM employee.employees WHERE _id = '64798c213f273a7ca2cf516c';"},
			wantErr: false,
		},
		{
			name:    "Second Insert - should not create new table for existing tables but handle nested insert",
			oplog:   parser.Oplog{Op: "i", Namespace: "employee.employees", Record: map[string]interface{}{"_id": "64798c213f273a7ca2cf516e", "address": []interface{}{map[string]interface{}{"line1": "481 Harborsburgh", "zip": "89799"}, map[string]interface{}{"line1": "329 Flatside", "zip": "80872"}}, "age": 37, "name": "Wilson Gleason", "phone": map[string]interface{}{"personal": "7678456640", "work": "8130097989"}, "position": "Manager", "salary": 5042.121824095532}, UpdateColumns: map[string]interface{}{"_id": "64798c213f273a7ca2cf516e"}, TableCreated: false, TimeStamp: map[string]interface{}{"I": 8, "T": 1.685687333e+09}},
			want:    []string{"INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf516e', 37, 'Wilson Gleason', 'Manager', 5042.121824095532);", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516e', '481 Harborsburgh', '89799');", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516e', '329 Flatside', '80872');", "INSERT INTO employee.employees_phone (_id, employees__id, personal, work) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf516e', '7678456640', '8130097989');"},
			wantErr: false,
		},
		{
			name:    "Insert with new column - Alter table with new column",
			oplog:   parser.Oplog{Op: "i", Namespace: "employee.employees", Record: map[string]interface{}{"_id": "64798c213f273a7ca2cf5172", "address": []interface{}{map[string]interface{}{"line1": "2787 Trackview", "zip": "23598"}, map[string]interface{}{"line1": "33659 South Mountainchester", "zip": "45086"}}, "age": 20, "name": "Delta Bahringer", "phone": map[string]interface{}{"personal": "9829848796", "work": "5636590993"}, "position": "Developer", "salary": 2980.1271103167737, "workhours": 6}, UpdateColumns: map[string]interface{}{"_id": "64798c213f273a7ca2cf5172"}, TableCreated: false, TimeStamp: map[string]interface{}{"I": 13, "T": 1.685687338e+09}},
			want:    []string{"ALTER TABLE employee.employees ADD workhours FLOAT;", "INSERT INTO employee.employees (_id, age, name, position, salary, workhours) VALUES ('64798c213f273a7ca2cf5172', 20, 'Delta Bahringer', 'Developer', 2980.1271103167737, 6);", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf5172', '2787 Trackview', '23598');", "INSERT INTO employee.employees_address (_id, employees__id, line1, zip) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf5172', '33659 South Mountainchester', '45086');", "INSERT INTO employee.employees_phone (_id, employees__id, personal, work) VALUES ('64798c213f273a7ca2cf516e', '64798c213f273a7ca2cf5172', '9829848796', '5636590993');"},
			wantErr: false,
		},
	}

	p := parser.NewParser()
	p.SetIDGenerator(fakeIDGenerator)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := p.GenerateSql(tt.oplog)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("GetSqlStatements() failed: %v", gotErr)
				} else if !strings.Contains(gotErr.Error(), "error reading collection OP value") {
					t.Errorf("unexpected error message: %v", gotErr)
				}
				return
			}

			if tt.wantErr {
				t.Fatal("GetSqlStatements() succeeded unexpectedly")
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSqlStatements() = %v, want %v", got, tt.want)
			}
		})
	}
}
