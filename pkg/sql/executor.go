package sql

import (
	"fmt"

	"github.com/go-db/pkg/index"
	"github.com/go-db/pkg/schema"
)

type Executor struct {
	tables map[string]*index.BTree
}

func NewExecutor() *Executor {
	return &Executor{
		tables: make(map[string]*index.BTree),
	}
}

func (e *Executor) RegisterTable(name string, tree *index.BTree) {
	e.tables[name] = tree
}

func (e *Executor) Execute(stmt *Statement) (string, error) {
	if stmt.Type == CreateTable {
		// Mocked out because we manually setup the tree right now in REPL for "test"
		return fmt.Sprintf("Table %s already known.", stmt.TableName), nil
	}

	tree, exists := e.tables[stmt.TableName]
	if !exists {
		return "", fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	switch stmt.Type {
	case Insert:
		record := &schema.Record{ID: stmt.ID, Data: stmt.Data}
		err := tree.Insert(record)
		if err != nil {
			return "", fmt.Errorf("insert error: %w", err)
		}
		return fmt.Sprintf("Inserted 1 row into %s.", stmt.TableName), nil

	case Select:
		record, err := tree.Find(stmt.ID)
		if err != nil {
			return "", fmt.Errorf("select error: %w", err)
		}
		return fmt.Sprintf("ID: %d | Data: %s", record.ID, record.Data), nil

	case Update:
		return "", fmt.Errorf("update not implemented (yet)")

	case Delete:
		return "", fmt.Errorf("delete not implemented (yet)")

	default:
		return "", fmt.Errorf("unknown statement type")
	}
}
