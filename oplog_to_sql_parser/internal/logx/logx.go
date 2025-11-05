package logx

import (
	"fmt"
	"log"
	"os"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func Info(format string, v ...interface{}) {
	_ = log.Output(2, "[INFO] "+fmt.Sprintf(format, v...))
}

func Warn(format string, v ...interface{}) {
	_ = log.Output(2, "[WARN] "+fmt.Sprintf(format, v...))
}

func Error(format string, v ...interface{}) {
	_ = log.Output(2, "[ERROR] "+fmt.Sprintf(format, v...))
}

func Fatal(format string, v ...interface{}) {
	_ = log.Output(2, "[FATAL] "+fmt.Sprintf(format, v...))
	os.Exit(1)
}
