package model

type Event interface {
	Source() SourceType
	Offset() Offset
}

type SourceType string

const (
	SourceMySQLBinlog SourceType = "mysql-binlog"
	SourcePostgresWal SourceType = "postgres-wal"
)

type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)
