package main

import (
	"reflect"
	"testing"
)

func Test_generateTree(t *testing.T) {
	type args struct {
		directory string
	}
	tests := []struct {
		name    string
		args    args
		want    TreeNode
		wantErr bool
	}{
		{
			name: "Just directory without any flags",
			args: args{
				directory: ".",
			},
			want: TreeNode{
				Name:  ".",
				IsDir: true,
				Children: []*TreeNode{
					{Name: "go.mod", IsDir: false},
					{Name: "go.sum", IsDir: false},
					{Name: "main_test", IsDir: false},
					{Name: "main.go", IsDir: false},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateTree(tt.args.directory)
			if (err != nil) != tt.wantErr {
				t.Errorf("genreteTree() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateTree() = %v, want %v", got, tt.want)
			}
		})
	}
}
