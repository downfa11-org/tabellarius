package model

type TxBoundaryKind string

const (
	TxBegin    TxBoundaryKind = "BEGIN"
	TxCommit   TxBoundaryKind = "COMMIT"
	TxRollback TxBoundaryKind = "ROLLBACK"
)

type TransactionBoundaryEvent struct {
	source SourceType
	offset Offset
	txID   string
	kind   TxBoundaryKind
}

func NewTransactionBoundaryEvent(source SourceType, offset Offset, txID string, kind TxBoundaryKind) *TransactionBoundaryEvent {
	return &TransactionBoundaryEvent{
		source: source,
		offset: offset,
		txID:   txID,
		kind:   kind,
	}
}

func (e *TransactionBoundaryEvent) Source() SourceType   { return e.source }
func (e *TransactionBoundaryEvent) Offset() Offset       { return e.offset }
func (e *TransactionBoundaryEvent) TxID() string         { return e.txID }
func (e *TransactionBoundaryEvent) Kind() TxBoundaryKind { return e.kind }

type TransactionEvent struct {
	source  SourceType
	offset  Offset
	txID    string
	changes []RowChange
}

func NewTransactionEvent(source SourceType, offset Offset, txID string, changes []RowChange) *TransactionEvent {
	return &TransactionEvent{
		source:  source,
		offset:  offset,
		txID:    txID,
		changes: changes,
	}
}

func (e *TransactionEvent) Source() SourceType   { return e.source }
func (e *TransactionEvent) Offset() Offset       { return e.offset }
func (e *TransactionEvent) TxID() string         { return e.txID }
func (e *TransactionEvent) Changes() []RowChange { return e.changes }
