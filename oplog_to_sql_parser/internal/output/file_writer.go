package output

import (
	"fmt"
	"os"
	"strings"
)

func openOrCreateFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error opening/creating file -> %s\n err: %s", fileName, err)
	}
	return file, nil
}

func WriteToFileActions(sql []string, fileName string) {
	var file *os.File
	var err error
	file, err = openOrCreateFile(fileName)
	if err != nil {
		fmt.Println("Error creating file", err)
	}
	defer file.Close()

	_, err = file.WriteString(strings.Join(sql, ", ") + "\n")
	if err != nil {
		fmt.Printf("error writing to output file -> %v\n", err)
	}

}
