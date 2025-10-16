package catalog

import (
	"context"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

type OperationCreateTable struct {
	Identifier table.Identifier
	Schema     *iceberg.Schema
	Opts       []CreateTableOpt
}

type OperationCommitTable struct {
	Identifier   table.Identifier
	Requirements []table.Requirement
	Updates      []table.Update
}

type OperationSetKVSidecar struct {
	Key   string
	Value string
}

type Operation interface {
	transactionOperation()
}

func (c *OperationCreateTable) transactionOperation()  {}
func (c *OperationCommitTable) transactionOperation()  {}
func (c *OperationSetKVSidecar) transactionOperation() {}

type TransactionCatalog interface {
	Catalog
	SetKVSidecar(ctx context.Context, key, value string) error
	GetKVSidecar(ctx context.Context, key string) (string, error)
	Transaction(ctx context.Context, operations []Operation) error
}

type FollowerCatalog interface {
	FollowCreateTable(ctx context.Context, staged *table.StagedTable) error
	FollowCommitTable(ctx context.Context, previousMetadataLocation *string, staged *table.StagedTable) error
}
