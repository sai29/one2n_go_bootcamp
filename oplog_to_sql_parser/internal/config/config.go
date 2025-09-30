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

var flagCfg = &Config{}

type Config struct {
	InputFile    string
	OutputFile   string
	InputUri     string
	OutputUri    string
	InputMethod  InputMethod
	OutputMethod OutputMethod
}
