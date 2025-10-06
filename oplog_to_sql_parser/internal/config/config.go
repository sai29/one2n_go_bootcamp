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

type Config struct {
	Input struct {
		InputFile   string
		InputUri    string
		InputMethod InputMethod
	}
	Output struct {
		OutputFile   string
		OutputUri    string
		OutputMethod OutputMethod
	}

	Db struct {
		Dsn          string
		MaxOpenConns int
		MaxIdleConns int
		MaxIdleTime  string
	}
}
