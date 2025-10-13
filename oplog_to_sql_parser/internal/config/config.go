package config

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
