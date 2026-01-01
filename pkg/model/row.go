package model

type RowChangeEvent interface {
	Event
	TxID() string
	Changes() []RowChange
}

type RowChange struct {
	Schema string
	Table  string
	Op     OpType
	Rows   []RowData
}

type RowData struct {
	PK     map[string]any `json:"pk,omitempty"`
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`
}
