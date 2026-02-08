package cursus

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/downfa11-org/cursus/test/publisher/config" // todo. updated cursus package
	"github.com/downfa11-org/cursus/test/publisher/producer"
)

type Publisher struct {
	pub *producer.Publisher
}

func NewCursusPublisher(addr string) *Publisher {
	cfg, err := config.LoadPublisherConfig() // "/config.yaml"
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	pub, err := producer.NewPublisher(cfg)
	if err != nil {
		log.Printf("Failed to create publisher: %v", err)
		return nil
	}

	return &Publisher{
		pub: pub,
	}
}

func (p *Publisher) Publish(evt model.Event) error {
	if p.pub == nil {
		return fmt.Errorf("broker publisher not initialized")
	}

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

	eventJSON, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = p.pub.PublishMessage(string(eventJSON))
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
