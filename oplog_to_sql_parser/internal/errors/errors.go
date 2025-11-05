package errors

type AppError struct {
	Err   error
	Fatal bool
}

func SendFatal(ch chan<- AppError, err error) {
	if err == nil {
		return
	}
	ch <- AppError{Err: err, Fatal: true}
}

func SendWarn(ch chan<- AppError, err error) {
	if err == nil {
		return
	}
	ch <- AppError{Err: err, Fatal: false}
}
