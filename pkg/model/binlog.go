package model

import "time"

type BinlogRowEvent struct {
	source  SourceType
	offset  Offset
	txID    string
	changes []RowChange
}

func NewBinlogRowEvent(source SourceType, offset Offset, txID string, changes []RowChange) *BinlogRowEvent {
	return &BinlogRowEvent{
		source:  source,
		offset:  offset,
		txID:    txID,
		changes: changes,
	}
}

func (e *BinlogRowEvent) Source() SourceType   { return e.source }
func (e *BinlogRowEvent) Offset() Offset       { return e.offset }
func (e *BinlogRowEvent) TxID() string         { return e.txID }
func (e *BinlogRowEvent) Changes() []RowChange { return e.changes }

type BinlogDDLEvent struct {
	source    SourceType
	offsetVal MySQLOffset
	txID      string
	query     string
	timestamp time.Time
}

func NewBinlogDDLEvent(src SourceType, offset MySQLOffset, txID, query string) *BinlogDDLEvent {
	return &BinlogDDLEvent{
		source:    src,
		offsetVal: offset,
		txID:      txID,
		query:     query,
		timestamp: time.Now().UTC(),
	}
}

func (e *BinlogDDLEvent) Source() SourceType { return e.source }
func (e *BinlogDDLEvent) Offset() Offset     { return e.offsetVal }
func (e *BinlogDDLEvent) TxID() string       { return e.txID }
func (e *BinlogDDLEvent) Query() string      { return e.query }
func (e *BinlogDDLEvent) Type() string       { return "ddl" }
