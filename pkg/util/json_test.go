package util

import (
	"os"
	"testing"

	"github.com/downfa11-org/tabellarius/pkg/model"
)

func TestSaveLoadJSON(t *testing.T) {
	tmp, _ := os.CreateTemp("", "json-*")
	defer os.Remove(tmp.Name())

	v := model.MySQLOffset{
		File: "binlog.1",
		Pos:  123,
	}

	if err := SaveJSON(tmp.Name(), v); err != nil {
		t.Fatalf("SaveJSON failed: %v", err)
	}

	loaded, ok := LoadJSON[model.MySQLOffset](tmp.Name())
	if !ok {
		t.Fatal("LoadJSON failed")
	}

	if loaded.File != v.File || loaded.Pos != v.Pos {
		t.Fatal("loaded value mismatch")
	}
}
