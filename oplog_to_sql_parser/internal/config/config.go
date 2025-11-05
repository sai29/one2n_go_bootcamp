package config

import (
	"fmt"
	"strings"
)

type InputMethod string
type OutputMethod string

const (
	InputFile InputMethod = "file"
	InputDb   InputMethod = "db"
)

const (
	OutputFile OutputMethod = "file"
	OutputDb   OutputMethod = "db"
)

type Output struct {
	OutputFile   string
	OutputUri    string
	OutputMethod OutputMethod
}

type Input struct {
	InputFile   string
	InputUri    string
	InputMethod InputMethod
}

type Config struct {
	Input  Input
	Output Output
}

func ValidateConfig(flags *Config) error {
	if bothInputFlagsPresent(flags) {
		return fmt.Errorf("error: cannot specify both --input-file or --input-uri")
	}

	if neitherInputFlagsPresent(flags) {
		return fmt.Errorf("error: must specify --input-file or --input-uri")
	}

	if bothOutputFlagsPresent(flags) {
		return fmt.Errorf("error: must specify --output-file or --output-uri")
	}

	if neitherOutputFlagsPresent(flags) {
		return fmt.Errorf("error: must specify --output-file or --output-uri")
	}

	if flags.Input.InputFile != "" {
		flags.Input.InputMethod = "file"
	} else {

		if !isValidMongoUri(flags.Input.InputUri) {
			return fmt.Errorf("error: invalid mongo oplog input uri")
		}

		flags.Input.InputMethod = "mongo"
	}

	if flags.Output.OutputFile != "" {
		flags.Output.OutputMethod = "file"
	} else {

		if !isValidPostgresUri(flags.Output.OutputUri) {
			return fmt.Errorf("error: invalid postgres db output uri")
		}

		flags.Output.OutputMethod = "postgres"
	}

	return nil
}

func isValidMongoUri(uri string) bool {
	return strings.HasPrefix(uri, "mongodb://")
}

func isValidPostgresUri(uri string) bool {
	return strings.HasPrefix(uri, "mongodb://")
}

func bothInputFlagsPresent(flags *Config) bool {
	return flags.Input.InputUri != "" && flags.Input.InputFile != ""
}

func neitherInputFlagsPresent(flags *Config) bool {
	return flags.Input.InputUri == "" && flags.Input.InputFile == ""
}

func bothOutputFlagsPresent(flags *Config) bool {
	return flags.Output.OutputUri != "" && flags.Output.OutputFile != ""
}

func neitherOutputFlagsPresent(flags *Config) bool {
	return flags.Output.OutputUri == "" && flags.Output.OutputFile == ""
}
