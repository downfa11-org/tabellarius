package inspector

import (
	"testing"

	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseDSN(t *testing.T) {
	b := &BinlogInspector{
		dsn: "user:pass@tcp(localhost:3307)/mydb",
	}

	if err := b.parseDSN(); err != nil {
		t.Fatalf("parseDSN failed: %v", err)
	}
	if b.user != "user" || b.password != "pass" {
		t.Fatalf("auth parse failed")
	}
	if b.host != "localhost" || b.port != 3307 {
		t.Fatalf("host parse failed: %s:%d", b.host, b.port)
	}
}

func TestEmitRowEvents_Write(t *testing.T) {
	out := make(chan model.Event, 1)

	b := &BinlogInspector{
		currentFile: "binlog.000001",
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"test.users": {
				columns: []string{"id", "name"},
			},
		},
	}

	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "alice"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.WRITE_ROWS_EVENTv2,
		LogPos:    123,
	}

	b.emitRowEvents(out, header, ev)
	close(out)

	got, ok := <-out
	if !ok {
		t.Fatalf("no event emitted")
	}

	rowEvt, ok := got.(*model.BinlogRowEvent)
	if !ok {
		t.Fatalf("unexpected event type: %T", got)
	}

	if rowEvt.TxID() != "tx-1" {
		t.Fatalf("unexpected txID: %s", rowEvt.TxID())
	}
	if len(rowEvt.Changes()) != 1 {
		t.Fatalf("expected 1 change, got %d", len(rowEvt.Changes()))
	}

	change := rowEvt.Changes()[0]
	if change.Op != model.OpInsert {
		t.Fatalf("expected insert op, got %s", change.Op)
	}
}

func TestEmitRowEvents_UpdateInvalid(t *testing.T) {
	out := make(chan model.Event, 1)

	b := &BinlogInspector{
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"test.users": {
				columns: []string{"id", "name"},
			},
		},
	}

	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "before"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.UPDATE_ROWS_EVENTv2,
		LogPos:    10,
	}

	b.emitRowEvents(out, header, ev)
	close(out)

	if _, ok := <-out; ok {
		t.Fatalf("expected no events for invalid update")
	}
}
