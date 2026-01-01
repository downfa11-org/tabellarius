package source

import (
	"context"

	"github.com/downfa11-org/tabellarius/pkg/inspector"
	"github.com/downfa11-org/tabellarius/pkg/model"
	"github.com/downfa11-org/tabellarius/pkg/source/cursus"
)

type TabellariusSource struct {
	ins inspector.Inspector[model.Event]
	pub *cursus.Publisher
}

func (s *TabellariusSource) Start(ctx context.Context) {
	ch := make(chan model.Event, 128)

	go func() {
		defer close(ch)
		_ = s.ins.Start(ctx, ch)
	}()

	go s.run(ctx, ch)
}

func (s *TabellariusSource) run(ctx context.Context, in <-chan model.Event) {
	txBuffer := map[string][]model.RowChange{}
	var lastOffset model.Offset
	var lastSource model.SourceType

	for {
		select {
		case <-ctx.Done():
			return

		case evt, ok := <-in:
			if !ok {
				return
			}

			lastOffset = evt.Offset()
			lastSource = evt.Source()

			switch e := evt.(type) {
			case model.RowChangeEvent:
				txBuffer[e.TxID()] = append(txBuffer[e.TxID()], e.Changes()...)
			case *model.BinlogDDLEvent:
				_ = s.pub.Publish(e)
			case *model.TransactionBoundaryEvent:
				switch e.Kind() {
				case model.TxCommit:
					changes := txBuffer[e.TxID()]
					if len(changes) == 0 {
						delete(txBuffer, e.TxID())
						continue
					}

					txEvt := model.NewTransactionEvent(lastSource, lastOffset, e.TxID(), changes)
					_ = s.pub.Publish(txEvt)
					delete(txBuffer, e.TxID())

				case model.TxRollback:
					delete(txBuffer, e.TxID())
				}
			}
		}
	}
}
