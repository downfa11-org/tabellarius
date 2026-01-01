package source

import (
	"log"

	"github.com/downfa11-org/tabellarius/pkg/config"
	"github.com/downfa11-org/tabellarius/pkg/inspector"
	"github.com/downfa11-org/tabellarius/pkg/model"
	"github.com/downfa11-org/tabellarius/pkg/source/cursus"
	"github.com/downfa11-org/tabellarius/pkg/util"
)

func NewFromConfig(cfg *config.Config) *TabellariusSource {
	switch cfg.Database.Type {
	case model.MySQL, model.MariaDB:
		return NewMySQLSource(cfg.Database.Type, cfg.Database.Schema, cfg.DSN(), cfg.CDCServer.OffsetFile, cfg.CDCServer.PublisherAddr, cfg.Tables)
	case model.Postgres:
		log.Fatal("postgres source not implemented")
	default:
		log.Fatalf("unsupported database type: %s", cfg.Database.Type)
	}
	return nil
}

func NewMySQLSource(dbType model.DatabaseType, dbSchema, dbDSN string, offsetPath string, pubAddr string, tables []config.Table) *TabellariusSource {
	binlogOffset := offsetPath + ".binlog"
	ins, err := inspector.NewBinlogInspector(dbType, dbSchema, dbDSN, binlogOffset, util.GenerateID(), tables)
	if err != nil {
		log.Fatal(err)
	}

	var inspector inspector.Inspector[model.Event] = ins

	return &TabellariusSource{
		ins: inspector,
		pub: cursus.NewCursusPublisher(pubAddr),
	}
}
