package model

import "time"

type BinlogRowEvent struct {
	source    SourceType
	timestamp time.Time
	offset    Offset
	txID      string
	changes   []RowChange
}

func NewBinlogRowEvent(source SourceType, offset Offset, timestmap time.Time, txID string, changes []RowChange) *BinlogRowEvent {
	return &BinlogRowEvent{
		source:    source,
		offset:    offset,
		timestamp: timestmap,
		txID:      txID,
		changes:   changes,
	}
}

func (e *BinlogRowEvent) Source() SourceType   { return e.source }
func (e *BinlogRowEvent) Timestamp() time.Time { return e.timestamp }
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

func NewBinlogDDLEvent(src SourceType, offset MySQLOffset, timestamp time.Time, txID, query string) *BinlogDDLEvent {
	return &BinlogDDLEvent{
		source:    src,
		offsetVal: offset,
		timestamp: timestamp,
		txID:      txID,
		query:     query,
	}
}

func (e *BinlogDDLEvent) Source() SourceType   { return e.source }
func (e *BinlogDDLEvent) Offset() Offset       { return e.offsetVal }
func (e *BinlogDDLEvent) Timestamp() time.Time { return e.timestamp }
func (e *BinlogDDLEvent) TxID() string         { return e.txID }
func (e *BinlogDDLEvent) Query() string        { return e.query }
func (e *BinlogDDLEvent) Type() string         { return "ddl" }
