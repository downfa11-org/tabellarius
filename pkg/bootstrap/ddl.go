package bootstrap

import (
	"database/sql"
	"fmt"

	"github.com/downfa11-org/tabellarius/pkg/config"
)

func Init(db *sql.DB, cfg *config.Config) error {
	cdcLog := cfg.CdcLog.Table
	ok, err := ExistsTable(db, cdcLog)
	if err != nil {
		return fmt.Errorf("failed to inspect cdc_log table: %w", err)
	}

	if !ok {
		if err := EnsureCDCLogTable(db, cdcLog); err != nil {
			return fmt.Errorf("failed to create cdc_log table: %w", err)
		}
		fmt.Printf("[cli] created table %s\n", cdcLog)
	}

	return nil
}

func Inspect(db *sql.DB, cfg *config.Config) (bool, error) {
	cdcLog := cfg.CdcLog.Table
	ok, err := ExistsTable(db, cdcLog)
	if err != nil {
		return false, err
	}
	Print("table "+cdcLog, ok)
	return ok, nil
}

func Print(label string, ok bool) {
	if ok {
		fmt.Printf("[OK] %s\n", label)
	} else {
		fmt.Printf("[MISSING] %s\n", label)
	}
}

func EnsureCDCLogTable(db *sql.DB, table string) error {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  seq BIGINT AUTO_INCREMENT PRIMARY KEY,
  table_name VARCHAR(64) NOT NULL,
  op ENUM('c','u','d') NOT NULL,
  row_id BIGINT NOT NULL,
  payload JSON NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`, table)

	_, err := db.Exec(ddl)
	return err
}

func ExistsTable(db *sql.DB, name string) (bool, error) {
	var v int
	err := db.QueryRow(`
SELECT 1 FROM information_schema.tables
WHERE table_schema = DATABASE()
AND table_name = ?`, name).Scan(&v)

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
