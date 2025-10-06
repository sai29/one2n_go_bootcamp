package output

type PostgresWriter struct {
	uri string
}

func NewPostgresWriter(uri string) *PostgresWriter {
	return &PostgresWriter{uri: uri}
}

func (mr *PostgresWriter) Write(sql string) error {
	return nil
}
