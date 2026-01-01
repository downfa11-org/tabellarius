package model

type DatabaseType string

const (
	MySQL    DatabaseType = "mysql"
	MariaDB  DatabaseType = "mariadb"
	Postgres DatabaseType = "postgres"
)

func (d DatabaseType) IsBinlogBased() bool {
	switch d {
	case MySQL, MariaDB:
		return true
	default:
		return false
	}
}

func (d DatabaseType) DriverName() string {
	switch d {
	case MySQL, MariaDB:
		return "mysql"
	case Postgres:
		return "postgres"
	default:
		panic("unsupported database type: " + string(d))
	}
}

func (d DatabaseType) BinlogFlavor() string {
	switch d {
	case MySQL:
		return "mysql"
	case MariaDB:
		return "mariadb"
	default:
		panic("binlog not supported: " + string(d))
	}
}
