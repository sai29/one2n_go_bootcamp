package output

import (
	"fmt"
	"os"
)

type FileWriter struct {
	uri string
}

func NewFileWriter(uri string) *FileWriter {
	return &FileWriter{uri: uri}
}

func (fr *FileWriter) Write(sql string) error {
	var file *os.File
	var err error
	file, err = openOrCreateFile(fr.uri)
	if err != nil {
		fmt.Println("Error creating file", err)
		return err
	}
	defer file.Close()

	_, err = file.WriteString(sql + "\n")
	if err != nil {
		fmt.Printf("error writing to output file -> %v\n", err)
		return err
	}
	return nil

}

func openOrCreateFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error opening/creating file -> %s\n err: %s", fileName, err)
	}
	return file, nil
}
