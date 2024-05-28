package basicnumfiltering

import (
	"reflect"
	"testing"
)

func TestEven(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns only even numbers",
			nums: []int{1, 2, 3, 4, 5, 6},
			want: []int{2, 4, 6},
		},
		{
			name: "returns empty array if no even numbers",
			nums: []int{1, 3, 5, 7, 9},
			want: []int{},
		},
		{
			name: "returns only even numbers if no odd numbers present",
			nums: []int{2, 4, 6, 8, 10, 12},
			want: []int{2, 4, 6, 8, 10, 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Even(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Even(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}

func TestOdd(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns only odd numbers",
			nums: []int{1, 2, 3, 4, 5, 6},
			want: []int{1, 3, 5},
		},
		{
			name: "returns empty array if no odd numbers",
			nums: []int{2, 4, 6, 8, 10},
			want: []int{},
		},
		{
			name: "returns only odd numbers if no even numbers present",
			nums: []int{1, 3, 5, 7, 9},
			want: []int{1, 3, 5, 7, 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Odd(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Odd(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}

func TestPrime(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns only prime numbers",
			nums: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			want: []int{2, 3, 5, 7},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{1, 4, 6, 8, 10},
			want: []int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Prime(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Odd(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}

func TestOddPrime(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns only prime numbers",
			nums: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			want: []int{3, 5, 7},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{1, 4, 6, 8, 10},
			want: []int{},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{41, 43, 45, 47, 51, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97},
			want: []int{41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OddPrime(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Odd(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}

func TestEvenAndFiveMultiples(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns only prime numbers",
			nums: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			want: []int{10},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			want: []int{10, 20},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{40, 41, 43, 45, 47, 51, 50, 53, 59, 60, 61, 67, 70, 71, 73, 79, 80, 83, 89, 97},
			want: []int{40, 50, 60, 70, 80},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EvenAndFiveMultiples(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Odd(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}

func TestOddAndThreeMultiplesGreaterThan10(t *testing.T) {
	tests := []struct {
		name string
		nums []int
		want []int
	}{
		{
			name: "returns empty array if no prime numbers",
			nums: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			want: []int{15},
		},
		{
			name: "returns empty array if no prime numbers",
			nums: []int{40, 41, 43, 45, 47, 51, 50, 53, 59, 63, 61, 66, 70, 71, 73, 78, 81, 83, 89, 97},
			want: []int{45, 51, 63, 81},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OddAndThreeMultiplesGreaterThan10(tt.nums); !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Odd(%v) = %v; want %v", tt.nums, got, tt.want)
			}
		})
	}
}
