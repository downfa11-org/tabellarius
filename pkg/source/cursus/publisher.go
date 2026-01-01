package cursus

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/downfa11-org/tabellarius/pkg/model"
)

type Publisher struct{}

func NewCursusPublisher(addr string) *Publisher {
	return &Publisher{}
}

func (p *Publisher) Publish(evt model.Event) error {
	prefix := fmt.Sprintf("[publish] source=%s offset=%s type=%T", evt.Source(), evt.Offset().String(), evt)

	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		log.Printf("%s [tx] kind=%s txID=%s", prefix, e.Kind(), e.TxID())
	case *model.BinlogDDLEvent:
		log.Printf("%s [ddl] txID=%s query=%s", prefix, e.TxID(), e.Query())
	case model.RowChangeEvent:
		changes := e.Changes()
		for ci, change := range changes {
			for ri, row := range change.Rows {
				if change.Op == model.OpUpdate && row.Before != nil && row.After != nil {
					beforeJSON, err := json.Marshal(row.Before)
					if err != nil {
						log.Printf("[publish] failed to marshal Before: %v", err)
						beforeJSON = []byte("{}")
					}

					afterJSON, err := json.Marshal(row.After)
					if err != nil {
						log.Printf("[publish] failed to marshal Before: %v", err)
						beforeJSON = []byte("{}")
					}

					log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=UPDATE before=%s after=%s", prefix, ci, ri, change.Schema, change.Table, e.TxID(), string(beforeJSON), string(afterJSON))
				} else {
					log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=%s", prefix, ci, ri, change.Schema, change.Table, e.TxID(), change.Op)
				}
			}
		}
	default:
		log.Printf("%s [unknown event]", prefix)
	}

	return nil
}
