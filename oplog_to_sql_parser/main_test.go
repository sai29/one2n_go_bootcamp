package main

import (
	"reflect"
	"testing"
)

func fakeIDGenerator(n int) string {
	return "64798c213f273a7ca2cf516e"
}
func TestParser_decodeJSONString(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		jsonOplog []string
		want      []string
		wantErr   bool
	}{
		{
			name: "Story 2 (parsing delete oplog)",
			jsonOplog: []string{`{
							"op": "d",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b"
							}
						}`},
			want:    []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},

		{
			name: "Story 3 (parsing delete oplog)",
			jsonOplog: []string{`{
							"op": "d",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b"
							}
						}`},
			want:    []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},

		{
			name: "Story 1 and 4 (create table with one oplog entry)",
			jsonOplog: []string{`{
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b",
								"name": "Selena Miller",
								"roll_no": 51,
								"is_graduated": false,
								"date_of_birth": "2000-01-30"
								}
							}`},
			want:    []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);"},
			wantErr: false,
		},
		{
			name: "Story 5 (create table with multiple oplog entries)",
			jsonOplog: []string{`{
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b",
								"name": "Selena Miller",
								"roll_no": 51,
								"is_graduated": false,
								"date_of_birth": "2000-01-30"
							}
						}`,
				`{
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "14798c213f273a7ca2cf5174",
								"name": "George Smith",
								"roll_no": 21,
								"is_graduated": true,
								"date_of_birth": "2001-03-23"
							}
						}`},
			want:    []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21);"},
			wantErr: false,
		},
		{
			name: "Story 6 (alter table with multiple oplog entries)",
			jsonOplog: []string{`  {
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b",
								"name": "Selena Miller",
								"roll_no": 51,
								"is_graduated": false,
								"date_of_birth": "2000-01-30"
							}
						}`,
				`{
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "14798c213f273a7ca2cf5174",
								"name": "George Smith",
								"roll_no": 21,
								"is_graduated": true,
								"date_of_birth": "2001-03-23",
								"phone": "+91-81254966457"
							}
						}`},
			want:    []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);", "ALTER TABLE test.student ADD phone VARCHAR(255);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, phone, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', '+91-81254966457', 21);"},
			wantErr: false,
		},
		{
			name: "Story 7 (handle nested Mongo documents)",
			jsonOplog: []string{`{
							"op": "i",
							"ns": "test.student",
							"o": {
								"_id": "635b79e231d82a8ab1de863b",
								"name": "Selena Miller",
								"roll_no": 51,
								"is_graduated": false,
								"date_of_birth": "2000-01-30",
								"address": [
									{
										"line1": "481 Harborsburgh",
										"zip": "89799"
									},
									{
										"line1": "329 Flatside",
										"zip": "80872"
									}
								],
								"phone": {
									"personal": "7678456640",
									"work": "8130097989"
								}
							}
						}`},
			want:    []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);", "CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));", "INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('64798c213f273a7ca2cf516e', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');", "INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('64798c213f273a7ca2cf516e', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');", "CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));", "INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('64798c213f273a7ca2cf516e', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser()
			p.idGenerator = fakeIDGenerator //fakeID set for tests since not possible to compare randomly generated ID from the code to test data.
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeJSONString() = %v, want %v", got, tt.want)
			}
		})
	}
}
