package basicnumfiltering

import "math"

func Filter(in []int, callback func(int) bool) []int {
	MatchingNums := []int{}
	for _, v := range in {
		if callback(v) {
			MatchingNums = append(MatchingNums, v)
		}
	}
	return MatchingNums
}

func Even(nums []int) []int {
	EvenNums := Filter(nums, func(num int) bool {
		return IsEven(num)
	})
	return EvenNums
}

func Odd(nums []int) []int {
	OddNums := Filter(nums, func(num int) bool {
		return IsOdd(num)
	})
	return OddNums
}

func Prime(nums []int) []int {
	PrimeNums := Filter(nums, func(num int) bool {
		if num > 1 {
			return IsPrime(num)
		} else {
			return false
		}
	})
	return PrimeNums
}

func IsEven(num int) bool {
	return num%2 == 0
}

func IsOdd(num int) bool {
	return num%2 != 0
}

func IsPrime(num int) bool {
	if num == 1 {
		return false
	}
	for j := 2; j <= int(math.Sqrt(float64(num))); j++ {
		if num%j == 0 {
			return false
		}
	}
	return true
}

func MultipleOf(num int, multiple int) bool {
	return num%multiple == 0
}

func Multiple(num int, multiple int, callback func(int, int) bool) bool {
	return callback(num, multiple)
}

func OddPrime(nums []int) []int {
	OddPrimes := Filter(nums, func(num int) bool {
		return IsPrime(num) && IsOdd(num)
	})
	return OddPrimes
}

func EvenAndFiveMultiples(nums []int) []int {
	MatchingNums := Filter(nums, func(num int) bool {
		return IsEven(num) && Multiple(num, 5, func(number, multiple int) bool {
			return number%multiple == 0
		})
	})
	return MatchingNums
}

func OddAndThreeMultiplesGreaterThan10(nums []int) []int {
	MatchingNums := Filter(nums, func(num int) bool {
		return IsOdd(num) && Multiple(num, 3, func(number, multiple int) bool {
			return number%multiple == 0
		}) && num > 10
	})
	return MatchingNums
}
