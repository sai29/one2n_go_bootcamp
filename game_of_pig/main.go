package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	player1, _ := strconv.Atoi(args[0])
	player2, _ := strconv.Atoi(args[1])

	playGame(map[string]int{"player1": player1, "player2": player2, "noOfGames": 10})
}

func playGame(playerHolds map[string]int) {

	cumulativePlayerScores := map[string]int{"player1": 0, "player2": 0}
	playerWins := map[string]int{"player1": 0, "player2": 0}

	for currentGame := 1; currentGame <= playerHolds["noOfGames"]; currentGame++ {
		currentPlayerActive := "player1"
		// fmt.Printf("Current game is %v\n", currentGame)
		currentTurnScore := 0
		for activeGame(cumulativePlayerScores) {
			// fmt.Printf("Current player scores are %v\n", cumulativePlayerScores)
			// fmt.Printf("Current active player is %v\n", currentPlayerActive)
			for currentTurnScore < playerHolds[currentPlayerActive] && currentTurnScore+cumulativePlayerScores[currentPlayerActive] < 100 {
				diceRoll := randRange(1, 6)
				// fmt.Printf("Current dice roll for %v is %v\n", currentPlayerActive, diceRoll)
				if diceRoll == 1 {
					currentTurnScore = 0
					break
				} else {
					currentTurnScore += diceRoll
				}
			}
			// fmt.Printf("Current turn score for %v is %v\n", currentPlayerActive, currentTurnScore)
			cumulativePlayerScores[currentPlayerActive] += currentTurnScore
			currentPlayerActive = setCurrentPlayer(currentPlayerActive)
			currentTurnScore = 0
		}
		playerWins = gameWinner(cumulativePlayerScores, playerWins)

		// fmt.Println(cumulativePlayerScores)
		currentTurnScore = 0
		cumulativePlayerScores["player1"], cumulativePlayerScores["player2"] = 0, 0
	}
	printResult(playerWins, playerHolds)
	// fmt.Println(playerWins)
}

func printResult(playerWins, playerHolds map[string]int) {
	player1WinPercentage := int32(float32(playerWins["player1"]) / float32(playerHolds["noOfGames"]) * 100.0)
	player2WinPercentage := int32(float32(playerWins["player2"]) / float32(playerHolds["noOfGames"]) * 100.0)
	fmt.Printf("Holding at %v vs Holding at %v: wins: %v/%v %v percent, losses: %v/%v %v percent \n", playerHolds["player1"], playerHolds["player2"],
		playerWins["player1"], playerHolds["noOfGames"], player1WinPercentage, playerWins["player2"], playerHolds["noOfGames"],
		player2WinPercentage)

}

func gameWinner(playerScores, playerWins map[string]int) map[string]int {
	if playerScores["player1"] >= 100 {
		playerWins["player1"] += 1
	} else {
		playerWins["player2"] += 1
	}
	return playerWins
}

func activeGame(playerScores map[string]int) bool {
	if playerScores["player1"] < 100 && playerScores["player2"] < 100 {
		return true
	}
	return false
}

func randRange(min, max int) int {
	return rand.Intn(max+1-min) + min
}

func setCurrentPlayer(currentPlayer string) string {
	if currentPlayer == "player1" {
		currentPlayer = "player2"
	} else if currentPlayer == "player2" {
		currentPlayer = "player1"
	}
	return currentPlayer
}
