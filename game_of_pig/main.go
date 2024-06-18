package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

func main() {
	args := os.Args[1:]

	if strings.Contains(args[1], "-") && strings.Contains(args[0], "-") {
		player1StartHold, _ := strconv.Atoi(strings.Split(args[0], "-")[0])
		player1EndHold, _ := strconv.Atoi(strings.Split(args[0], "-")[1])

		player2StartHold, _ := strconv.Atoi(strings.Split(args[1], "-")[0])
		player2EndHold, _ := strconv.Atoi(strings.Split(args[1], "-")[1])

		for i := player1StartHold; i <= player1EndHold; i++ {
			playerWins := map[string]int{"player1": 0, "player2": 0}
			for j := player2StartHold; j <= player2EndHold; j++ {
				if i == j {
					continue
				} else {
					gameResult := playGame(map[string]int{"player1": i, "player2": j, "noOfGames": 10}, map[string]bool{"Print": false})
					playerWins["player1"] += gameResult["player1"]
					playerWins["player2"] += gameResult["player2"]

				}

			}
			printGameResultForVariedHolds(i, playerWins)

		}

	} else if strings.Contains(args[1], "-") && !strings.Contains(args[0], "-") {
		player1, _ := strconv.Atoi(args[0])
		startHold, _ := strconv.Atoi(strings.Split(args[1], "-")[0])
		endHold, _ := strconv.Atoi(strings.Split(args[1], "-")[1])
		for i := startHold; i <= endHold; i++ {
			if i == player1 {
				continue
			} else {
				playGame(map[string]int{"player1": player1, "player2": i, "noOfGames": 10}, map[string]bool{"Print": true})
			}
		}
	} else if !strings.Contains(args[1], "-") && !strings.Contains(args[0], "-") {
		player1, _ := strconv.Atoi(args[0])
		player2, _ := strconv.Atoi(args[1])
		playGame(map[string]int{"player1": player1, "player2": player2, "noOfGames": 10}, map[string]bool{"Print": true})
	}
}

func printGameResultForVariedHolds(player1Hold int, playerWins map[string]int) {
	player1WinPercentage := int32((float32(playerWins["player1"]) / 990.0) * 100.0)
	player2WinPercentage := int32((float32(playerWins["player2"]) / 990.0) * 100.0)
	fmt.Printf("Result: Wins, losses staying at k =  %v: %v/990 (%v percent), %v/990 (%v percent) \n", player1Hold, playerWins["player1"], player1WinPercentage,
		playerWins["player2"], player2WinPercentage)

}

func playGame(playerHolds map[string]int, printResult map[string]bool) map[string]int {

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
	if printResult["Print"] {
		printPlayerWins(playerWins, playerHolds)
	}

	return playerWins
}

func printPlayerWins(playerWins, playerHolds map[string]int) {
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
