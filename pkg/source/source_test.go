package source

import (
	"testing"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/model"
)

func TestNewFromConfig_MySQL(t *testing.T) {
	cfg := &config.Config{
		Database: config.Database{
			Type:   model.MySQL,
			Schema: "test",
		},
		CDCServer: config.CDCServer{
			OffsetFile:    "/tmp/offset",
			PublisherAddr: "localhost:1234",
		},
	}

	src := NewFromConfig(nil, cfg)
	if src == nil {
		t.Fatal("expected source, got nil")
	}
}
