package output

type Writer interface {
	Write(sql string) error
}
