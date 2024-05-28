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

// func Even(nums []int) []int {
// 	EvenNums := []int{}
// 	for _, v := range nums {
// 		if IsEven(v) {
// 			EvenNums = append(EvenNums, v)
// 		}
// 	}
// 	return EvenNums
// }

// func Even(nums []int) []int {
// 	EvenNums := Filter(nums, func(num int) bool {
// 		return num%2 == 0
// 	})
// 	return EvenNums
// }

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

// func Prime(nums []int) []int {
// 	PrimeNums := []int{}
// 	for _, i := range nums {
// 		if i > 1 {
// 			if IsPrime(i) {
// 				PrimeNums = append(PrimeNums, i)
// 			}
// 		}
// 	}
// 	return PrimeNums
// }

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

//	func IsEven(num int) func(int) bool {
//		return func(num int) bool {
//			return num%2 == 0
//		}
//	}

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

func OddPrime(nums []int) []int {
	OddPrimes := Filter(nums, func(num int) bool {
		return IsPrime(num) && IsOdd(num)
	})
	return OddPrimes
}

func EvenAndFiveMultiples(nums []int) []int {
	MatchingNums := Filter(nums, func(num int) bool {
		return IsEven(num) && MultipleOf(num, 5)
	})
	return MatchingNums
}

func OddAndThreeMultiplesGreaterThan10(nums []int) []int {
	MatchingNums := Filter(nums, func(num int) bool {
		return IsOdd(num) && MultipleOf(num, 3) && num > 10
	})
	return MatchingNums
}
