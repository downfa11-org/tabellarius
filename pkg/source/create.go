package source

import (
	"database/sql"
	"log"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/inspector"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/source/cursus"
	"github.com/cursus-io/tabellarius/pkg/util"
)

func NewFromConfig(db *sql.DB, cfg *config.Config) *TabellariusSource {
	switch cfg.Database.Type {
	case model.MySQL, model.MariaDB:
		return NewMySQLSource(db, cfg.Database.Type, cfg.Database.Schema, cfg.DSN(), cfg.CDCServer.OffsetFile, cfg.CDCServer.PublisherAddr, cfg.Tables)
	case model.Postgres:
		log.Fatal("postgres source not implemented")
	default:
		log.Fatalf("unsupported database type: %s", cfg.Database.Type)
	}
	return nil
}

func NewMySQLSource(db *sql.DB, dbType model.DatabaseType, dbSchema, dbDSN string, offsetPath string, pubAddr string, tables []config.Table) *TabellariusSource {
	binlogOffset := offsetPath + ".binlog"
	ins, err := inspector.NewBinlogInspector(db, dbType, dbSchema, dbDSN, binlogOffset, util.GenerateID(), tables)
	if err != nil {
		log.Fatal(err)
	}

	var inspector inspector.Inspector[model.Event] = ins

	return &TabellariusSource{
		ins: inspector,
		pub: cursus.NewCursusPublisher(pubAddr),
	}
}
