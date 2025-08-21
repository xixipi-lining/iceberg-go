package catalog

import (
	"context"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

type CreateTableRequest struct {
	Identifier table.Identifier
	Schema     *iceberg.Schema
	Opts       []CreateTableOpt
}

type CommitTableRequest struct {
	Table        *table.Table
	Requirements []table.Requirement
	Updates      []table.Update
}

type SetKVSidecarRequest struct {
	Key   string
	Value string
}

type TransactionRequest interface {
	transactionRequest()
}

func (c *CreateTableRequest) transactionRequest()  {}
func (c *CommitTableRequest) transactionRequest()  {}
func (c *SetKVSidecarRequest) transactionRequest() {}

type FollowerCatalog interface {
	CreateTableByTable(ctx context.Context, staged *table.StagedTable) error
	CommitTableByTable(ctx context.Context, current *table.Table, staged *table.StagedTable) error
}

type TransactionCatalog interface {
	Catalog
	SetKVSidecar(ctx context.Context, key, value string) error
	GetKVSidecar(ctx context.Context, key string) (string, error)
	Transaction(ctx context.Context, reqs []TransactionRequest, follwer ...FollowerCatalog) error
}